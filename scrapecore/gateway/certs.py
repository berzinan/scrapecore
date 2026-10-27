"""
scrapecore/gateway/certs.py

Self-signed certificate helpers for the gateway. No domain or certificate
authority is needed: agents pin the certificate's SHA-256 fingerprint.
"""
from __future__ import annotations

import hashlib
import shutil
import ssl
import subprocess
from pathlib import Path


def cert_fingerprint(cert_path: str | Path) -> str:
    """SHA-256 fingerprint of a PEM certificate, as 64 lowercase hex digits."""
    der = ssl.PEM_cert_to_DER_cert(Path(cert_path).read_text())
    return hashlib.sha256(der).hexdigest()


def generate_self_signed(cert_path: str | Path, key_path: str | Path, days: int = 3650) -> str:
    """
    Create an EC P-256 self-signed certificate and key with openssl.
    :return: the certificate's SHA-256 fingerprint.
    """
    if shutil.which("openssl") is None:
        raise RuntimeError("openssl not found (on Windows, run this inside WSL)")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-nodes",
            "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
            "-keyout", str(key_path), "-out", str(cert_path),
            "-days", str(days), "-subj", "/CN=scrapecore-gateway",
        ],
        check=True,
        capture_output=True,
    )
    Path(key_path).chmod(0o600)
    return cert_fingerprint(cert_path)


def server_ssl_context(cert_path: str | Path, key_path: str | Path) -> ssl.SSLContext:
    """TLS context for serving the gateway with the given certificate."""
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(str(cert_path), str(key_path))
    return context
