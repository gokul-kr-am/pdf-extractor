#!/usr/bin/env python3
"""Extract material table details from PDF to Excel (.xlsx).

This script uses only Python standard library modules.
"""

from __future__ import annotations

import argparse
import html
import re
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


STREAM_RE = re.compile(rb"stream\r?\n")
BT_ET_RE = re.compile(rb"BT(.*?)ET", re.S)
TM_RE = re.compile(
    rb"([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+Tm"
)
Tj_RE = re.compile(rb"\((.*?)(?<!\\)\)\s*Tj", re.S)
TJ_RE = re.compile(rb"\[(.*?)\]\s*TJ", re.S)
TJ_STR_RE = re.compile(rb"\((.*?)(?<!\\)\)", re.S)


@dataclass
class TextToken:
    x: float
    y: float
    text: str


@dataclass
class MaterialRow:
    section: str
    category: str
    pt_no: str
    description: str
    size_inch: str
    commodity: str
    code: str
    qty: str


class PdfTextExtractor:
    def __init__(self, pdf_path: Path) -> None:
        self.pdf_path = pdf_path

    @staticmethod
    def _decode_pdf_string(data: bytes) -> str:
        out = bytearray()
        i = 0
        while i < len(data):
            c = data[i]
            if c == 92:  # backslash escape
                i += 1
                if i >= len(data):
                    break
                d = data[i]
                simple_escapes = {
                    ord("n"): 10,
                    ord("r"): 13,
                    ord("t"): 9,
                    ord("b"): 8,
                    ord("f"): 12,
                    ord("("): ord("("),
                    ord(")"): ord(")"),
                    ord("\\"): ord("\\"),
                }
                if d in simple_escapes:
                    out.append(simple_escapes[d])
                    i += 1
                    continue
                if 48 <= d <= 55:  # octal escape
                    oct_digits = [d]
                    i += 1
                    for _ in range(2):
                        if i < len(data) and 48 <= data[i] <= 55:
                            oct_digits.append(data[i])
                            i += 1
                        else:
                            break
                    out.append(int(bytes(oct_digits), 8))
                    continue
                out.append(d)
                i += 1
                continue

            out.append(c)
            i += 1

        # PDF text is often WinAnsi; latin1 is a safe byte-preserving decode.
        return out.decode("latin1", errors="ignore")

    def extract_tokens(self) -> list[TextToken]:
        raw = self.pdf_path.read_bytes()
        tokens: list[TextToken] = []

        for stream_match in STREAM_RE.finditer(raw):
            start = stream_match.end()
            end = raw.find(b"endstream", start)
            if end < 0:
                continue

            stream_data = raw[start:end].rstrip(b"\r\n")
            if not stream_data:
                continue

            try:
                decoded = zlib.decompress(stream_data)
            except zlib.error:
                continue

            if b"BT" not in decoded:
                continue

            for block_match in BT_ET_RE.finditer(decoded):
                block = block_match.group(1)
                tm = TM_RE.search(block)
                if not tm:
                    continue

                x = float(tm.group(5))
                y = float(tm.group(6))

                parts: list[str] = []
                for m in Tj_RE.finditer(block):
                    parts.append(self._decode_pdf_string(m.group(1)))

                for m in TJ_RE.finditer(block):
                    arr = m.group(1)
                    for sm in TJ_STR_RE.finditer(arr):
                        parts.append(self._decode_pdf_string(sm.group(1)))

                text = "".join(parts).strip()
                if text:
                    tokens.append(TextToken(x=x, y=y, text=text))

        return tokens


class MaterialTableParser:
    """Heuristic parser tailored to typical piping material table drawings."""

    # Column split positions inferred from sample drawing
    PT_MAX_X = 1282.0
    DESC_MIN_X = 1282.0
    DESC_MAX_X = 1488.0
    SIZE_MIN_X = 1488.0
    SIZE_MAX_X = 1518.0
    COM_MIN_X = 1518.0
    COM_MAX_X = 1568.0
    CODE_MIN_X = 1568.0
    CODE_MAX_X = 1611.0
    QTY_MIN_X = 1611.0

    def __init__(self, tokens: Iterable[TextToken]) -> None:
        self.tokens = sorted(tokens, key=lambda t: (-t.y, t.x))

    @staticmethod
    def _bucket_lines(tokens: list[TextToken], y_tol: float = 1.3) -> list[list[TextToken]]:
        lines: list[list[TextToken]] = []
        current: list[TextToken] = []
        current_y: float | None = None

        for t in tokens:
            if current_y is None:
                current = [t]
                current_y = t.y
                continue

            if abs(t.y - current_y) <= y_tol:
                current.append(t)
            else:
                lines.append(sorted(current, key=lambda v: v.x))
                current = [t]
                current_y = t.y

        if current:
            lines.append(sorted(current, key=lambda v: v.x))

        return lines

    @staticmethod
    def _join_text(parts: list[str]) -> str:
        s = " ".join(p for p in parts if p)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _classify_token(self, token: TextToken) -> str:
        x = token.x
        if x < self.PT_MAX_X:
            return "pt_no"
        if self.DESC_MIN_X <= x < self.DESC_MAX_X:
            return "description"
        if self.SIZE_MIN_X <= x < self.SIZE_MAX_X:
            return "size_inch"
        if self.COM_MIN_X <= x < self.COM_MAX_X:
            return "commodity"
        if self.CODE_MIN_X <= x < self.CODE_MAX_X:
            return "code"
        return "qty"

    def parse(self) -> list[MaterialRow]:
        # Focus on right-side material table zone; avoid unrelated title block text.
        table_tokens = [t for t in self.tokens if t.x >= 1260.0]
        lines = self._bucket_lines(table_tokens)

        rows: list[MaterialRow] = []
        section = ""
        category = ""
        current: MaterialRow | None = None

        for line in lines:
            text_line = self._join_text([t.text for t in line]).upper()
            if not text_line:
                continue

            if "FABRICATION" in text_line and "MATERIAL" in text_line:
                if current:
                    rows.append(current)
                    current = None
                section = "FABRICATION MATERIALS"
                continue
            if "ERECTION" in text_line and "MATERIAL" in text_line:
                if current:
                    rows.append(current)
                    current = None
                section = "ERECTION MATERIALS"
                continue

            if "ISSUED FOR CONSTRUCTION" in text_line and section:
                break

            # Skip headers and continuation markers
            header_markers = (
                "DESCRIPTION",
                "COMMODITY",
                "(INCH)",
                "PT.",
                "NO.",
                "QTY.",
                "SIZE",
                "CODE",
                "CONT.",
            )
            if any(h in text_line for h in header_markers):
                continue

            # Category lines (e.g., FITTINGS / FLANGES / BOLTS / VALVES ITEMS)
            cols = {
                "pt_no": [],
                "description": [],
                "size_inch": [],
                "commodity": [],
                "code": [],
                "qty": [],
            }
            for t in line:
                cols[self._classify_token(t)].append(t.text)

            pt_no = self._join_text(cols["pt_no"])
            description = self._join_text(cols["description"])
            size_inch = self._join_text(cols["size_inch"])
            commodity = self._join_text(cols["commodity"])
            code = self._join_text(cols["code"])
            qty = self._join_text(cols["qty"])

            category_guess = ""
            desc_upper = description.upper()
            if "FITTING" in desc_upper:
                category_guess = "FITTINGS"
            elif "FLANGE" in desc_upper:
                category_guess = "FLANGES"
            elif "BOLT" in desc_upper:
                category_guess = "BOLTS"
            elif "VALVE" in desc_upper:
                category_guess = "VALVES"

            is_category_line = (
                bool(category_guess)
                and not pt_no
                and not qty
                and not size_inch
                and not commodity
                and len(description.split()) <= 4
            )
            if is_category_line:
                # New subheading inside the same section. Close any active row first.
                if current:
                    rows.append(current)
                    current = None
                category = category_guess
                continue

            is_new_item = bool(pt_no and re.fullmatch(r"\d+", pt_no) and qty)

            if is_new_item:
                if current:
                    rows.append(current)
                current = MaterialRow(
                    section=section,
                    category=category,
                    pt_no=pt_no,
                    description=description,
                    size_inch=size_inch,
                    commodity=commodity,
                    code=code,
                    qty=qty,
                )
                continue

            # Continuation line for previous item
            if current:
                if description:
                    current.description = self._join_text([current.description, description])
                if size_inch:
                    current.size_inch = self._join_text([current.size_inch, size_inch])
                if commodity:
                    current.commodity = self._join_text([current.commodity, commodity])
                if code:
                    current.code = self._join_text([current.code, code])
                if qty and not current.qty:
                    current.qty = qty

        if current:
            rows.append(current)

        return rows


class XlsxWriter:
    @staticmethod
    def _col_name(index: int) -> str:
        out = ""
        i = index
        while i > 0:
            i, rem = divmod(i - 1, 26)
            out = chr(65 + rem) + out
        return out

    @classmethod
    def _cell(cls, row_idx: int, col_idx: int, value: str) -> str:
        ref = f"{cls._col_name(col_idx)}{row_idx}"
        escaped = html.escape(value)
        return f'<c r="{ref}" t="inlineStr"><is><t>{escaped}</t></is></c>'

    @classmethod
    def write(cls, rows: list[MaterialRow], out_path: Path) -> None:
        headers = [
            "Section",
            "Category",
            "PT No",
            "Description",
            "Size (Inch)",
            "Commodity",
            "Code",
            "Qty",
        ]

        sheet_rows: list[str] = []

        # Header row
        header_cells = [cls._cell(1, i + 1, h) for i, h in enumerate(headers)]
        sheet_rows.append(f'<row r="1">{"".join(header_cells)}</row>')

        for idx, item in enumerate(rows, start=2):
            values = [
                item.section,
                item.category,
                item.pt_no,
                item.description,
                item.size_inch,
                item.commodity,
                item.code,
                item.qty,
            ]
            cells = [cls._cell(idx, c_idx + 1, v) for c_idx, v in enumerate(values)]
            sheet_rows.append(f'<row r="{idx}">{"".join(cells)}</row>')

        sheet_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetData>'
            + "".join(sheet_rows)
            + "</sheetData></worksheet>"
        )

        workbook_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="Materials" sheetId="1" r:id="rId1"/></sheets></workbook>'
        )

        rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
            'Target="xl/workbook.xml"/>'
            "</Relationships>"
        )

        workbook_rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            'Target="worksheets/sheet1.xml"/>'
            "</Relationships>"
        )

        content_types_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            "</Types>"
        )

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", content_types_xml)
            zf.writestr("_rels/.rels", rels_xml)
            zf.writestr("xl/workbook.xml", workbook_xml)
            zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
            zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract material details from a PDF and write them to an Excel file."
    )
    parser.add_argument("input_pdf", type=Path, help="Path to source PDF")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("materials.xlsx"),
        help="Output .xlsx path (default: materials.xlsx)",
    )
    args = parser.parse_args()

    extractor = PdfTextExtractor(args.input_pdf)
    tokens = extractor.extract_tokens()
    if not tokens:
        raise SystemExit("No extractable text tokens found in PDF.")

    table_parser = MaterialTableParser(tokens)
    rows = table_parser.parse()
    if not rows:
        raise SystemExit(
            "No material rows were detected. Try a clearer PDF or adjust parser thresholds."
        )

    XlsxWriter.write(rows, args.output)
    print(f"Extracted {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
