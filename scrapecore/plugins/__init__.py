# Data Formats
from scrapecore.plugins.base import (
    BaseParser,
    with_stage_output,
)
# Parser Registration
from scrapecore.plugins.registry import (
    ParserRegistry,
)
#Authentication / Session Management
from scrapecore.plugins.auth import BaseAuth


__all__ = [
    "BaseParser",
    "with_stage_output",
    "ParserRegistry",
    "BaseAuth"
]