"""
scrapecore-gateway — run the gateway and manage agent tokens.

    scrapecore-gateway gen-cert  --cert cert.pem --key key.pem
    scrapecore-gateway serve     --cert cert.pem --key key.pem [--port 8443]
    scrapecore-gateway add-agent laptop-anna      # prints the token once
    scrapecore-gateway revoke-agent laptop-anna
    scrapecore-gateway list-agents
    scrapecore-gateway fingerprint --cert cert.pem

Redis connection settings come from REDIS_URL / REDIS_HOST / REDIS_PORT /
REDIS_DB / REDIS_PASSWORD, as for the coordinator.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from aiohttp import web

from scrapecore.gateway.certs import cert_fingerprint, generate_self_signed, server_ssl_context
from scrapecore.gateway.server import create_gateway_app
from scrapecore.gateway.tokens import AgentTokenStore
from scrapecore.queue.redis_client import create_redis_client


async def _with_tokens(namespace: str, action):
    redis = create_redis_client()
    try:
        return await action(AgentTokenStore(redis, namespace))
    finally:
        await redis.aclose()


def _serve(args: argparse.Namespace) -> None:
    if args.insecure_http:
        ssl_context = None
        logging.warning("Serving plain HTTP: for local testing only, never expose this.")
    else:
        if not (args.cert and args.key):
            sys.exit("serve needs --cert and --key (or --insecure-http for local testing)")
        ssl_context = server_ssl_context(args.cert, args.key)

    async def make_app() -> web.Application:
        redis = create_redis_client()
        app = create_gateway_app(redis, args.namespace, min_agent_version=args.min_agent_version)

        async def close_redis(_app: web.Application) -> None:
            await redis.aclose()

        app.on_cleanup.append(close_redis)
        return app

    web.run_app(make_app(), host=args.host, port=args.port, ssl_context=ssl_context)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="scrapecore-gateway", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="default", help="must match the coordinator")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the gateway")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8443)
    serve.add_argument("--cert")
    serve.add_argument("--key")
    serve.add_argument("--min-agent-version", help="refuse older agents (HTTP 426)")
    serve.add_argument("--insecure-http", action="store_true", help="plain HTTP, local testing only")

    gen = sub.add_parser("gen-cert", help="create a self-signed certificate (needs openssl)")
    gen.add_argument("--cert", default="gateway-cert.pem")
    gen.add_argument("--key", default="gateway-key.pem")
    gen.add_argument("--days", type=int, default=3650)

    fp = sub.add_parser("fingerprint", help="print a certificate's SHA-256 fingerprint")
    fp.add_argument("--cert", default="gateway-cert.pem")

    add = sub.add_parser("add-agent", help="issue (or rotate) an agent token")
    add.add_argument("agent_id")
    revoke = sub.add_parser("revoke-agent", help="invalidate an agent token")
    revoke.add_argument("agent_id")
    sub.add_parser("list-agents", help="list agents with a valid token")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.command == "serve":
        _serve(args)
    elif args.command == "gen-cert":
        fingerprint = generate_self_signed(args.cert, args.key, args.days)
        print(f"Wrote {args.cert} and {args.key}")
        print(f"Fingerprint (give this to agents): {fingerprint}")
    elif args.command == "fingerprint":
        print(cert_fingerprint(args.cert))
    elif args.command == "add-agent":
        token = asyncio.run(_with_tokens(args.namespace, lambda s: s.add_agent(args.agent_id)))
        print(f"Token for {args.agent_id} (shown once, store it in the agent's config):")
        print(token)
    elif args.command == "revoke-agent":
        ok = asyncio.run(_with_tokens(args.namespace, lambda s: s.revoke(args.agent_id)))
        print(f"Revoked {args.agent_id}" if ok else f"{args.agent_id} had no token")
    elif args.command == "list-agents":
        for agent_id in asyncio.run(_with_tokens(args.namespace, lambda s: s.list_agents())):
            print(agent_id)


if __name__ == "__main__":
    main()
