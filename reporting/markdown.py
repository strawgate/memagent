from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Literal

MarkdownTableAlign = Literal["left", "center", "right"]

_ALIGN_DIVIDER: dict[MarkdownTableAlign, str] = {
    "left": "---",
    "center": ":---:",
    "right": "---:",
}


def _escape_cell(value: object) -> str:
    text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def markdown_table(
    *,
    headers: Sequence[str],
    rows: Iterable[Sequence[object]],
    align: Sequence[MarkdownTableAlign] | None = None,
) -> list[str]:
    if not headers:
        raise ValueError("headers must not be empty")
    if align is not None and len(align) != len(headers):
        raise ValueError("align length must match headers length")

    effective_align = align or ["left"] * len(headers)
    divider_cells = [_ALIGN_DIVIDER[cell_align] for cell_align in effective_align]

    lines = [
        f"| {' | '.join(_escape_cell(header) for header in headers)} |",
        f"| {' | '.join(divider_cells)} |",
    ]
    for row in rows:
        if len(row) != len(headers):
            raise ValueError("row length must match headers length")
        lines.append(f"| {' | '.join(_escape_cell(cell) for cell in row)} |")
    return lines
