#!/usr/bin/env python3
"""Extract material table details from PDF to Excel (.xlsx)."""

from __future__ import annotations

import argparse
import html
import re
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - dependency availability handled at runtime
    PdfReader = None


STREAM_RE = re.compile(rb"stream\r?\n")
BT_ET_RE = re.compile(rb"BT(.*?)ET", re.S)
TM_RE = re.compile(
    rb"([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+([\-0-9.]+)\s+Tm"
)
Tj_RE = re.compile(rb"\((.*?)(?<!\\)\)\s*Tj", re.S)
TJ_RE = re.compile(rb"\[(.*?)\]\s*TJ", re.S)
TJ_STR_RE = re.compile(rb"\((.*?)(?<!\\)\)", re.S)
INVALID_XML_CHARS_RE = re.compile(
    r"[\x00-\x08\x0B\x0C\x0E-\x1F\uD800-\uDFFF\uFFFE\uFFFF]"
)


@dataclass
class TextToken:
    group: int
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

    def _extract_tokens_pypdf(self) -> list[TextToken]:
        if PdfReader is None:
            return []

        tokens: list[TextToken] = []
        try:
            reader = PdfReader(str(self.pdf_path))
        except Exception:
            return []

        for page_idx, page in enumerate(reader.pages):
            page_tokens: list[TextToken] = []

            def visitor_text(text: str, cm: object, tm: object, font_dict: object, font_size: object) -> None:
                if not text:
                    return

                try:
                    x = float(tm[4])  # type: ignore[index]
                    y = float(tm[5])  # type: ignore[index]
                except Exception:
                    x = 0.0
                    y = 0.0

                for chunk in str(text).splitlines():
                    cleaned = chunk.strip()
                    if cleaned:
                        page_tokens.append(TextToken(group=page_idx, x=x, y=y, text=cleaned))

            try:
                page.extract_text(visitor_text=visitor_text)
            except Exception:
                continue

            tokens.extend(page_tokens)

        return tokens

    def _extract_stream_token_groups(self) -> list[list[TextToken]]:
        raw = self.pdf_path.read_bytes()
        groups: list[list[TextToken]] = []

        for group_idx, stream_match in enumerate(STREAM_RE.finditer(raw)):
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

            stream_tokens: list[TextToken] = []
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
                    stream_tokens.append(TextToken(group=group_idx, x=x, y=y, text=text))

            if stream_tokens:
                groups.append(stream_tokens)

        return groups

    def extract_token_groups(self) -> list[list[TextToken]]:
        tokens = self.extract_tokens()
        groups: list[list[TextToken]] = []
        current_group: int | None = None
        current: list[TextToken] = []

        for token in tokens:
            if current_group is None or token.group != current_group:
                if current:
                    groups.append(current)
                current = [token]
                current_group = token.group
                continue
            current.append(token)

        if current:
            groups.append(current)
        return groups

    def extract_tokens(self) -> list[TextToken]:
        tokens = self._extract_tokens_pypdf()
        if tokens:
            return tokens

        # Fallback path for unusual files where pypdf returns no positioned text.
        groups = self._extract_stream_token_groups()
        return [token for group in groups for token in group]


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
        # Preserve stream/page progression first, then line geometry within each group.
        self.tokens = sorted(tokens, key=lambda t: (t.group, -t.y, t.x))

    @staticmethod
    def _bucket_lines(tokens: list[TextToken], y_tol: float = 1.3) -> list[list[TextToken]]:
        lines: list[list[TextToken]] = []
        current: list[TextToken] = []
        current_y: float | None = None
        current_group: int | None = None

        for t in tokens:
            if current_y is None:
                current = [t]
                current_y = t.y
                current_group = t.group
                continue

            if t.group != current_group:
                lines.append(sorted(current, key=lambda v: v.x))
                current = [t]
                current_y = t.y
                current_group = t.group
                continue

            if abs(t.y - current_y) <= y_tol:
                current.append(t)
            else:
                lines.append(sorted(current, key=lambda v: v.x))
                current = [t]
                current_y = t.y
                current_group = t.group

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
                if current:
                    rows.append(current)
                    current = None
                section = ""
                category = ""
                continue

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


class PlainTextMaterialParser:
    """Fallback parser using plain extracted text when positional parsing fails."""

    ITEM_RE = re.compile(r"^\s*(\d+)\s+(.+?)\s+(\d+(?:\.\d+)?)\s*$")

    @classmethod
    def parse_pdf(cls, pdf_path: Path) -> list[MaterialRow]:
        if PdfReader is None:
            return []

        try:
            reader = PdfReader(str(pdf_path))
        except Exception:
            return []

        rows: list[MaterialRow] = []
        section = ""
        category = ""

        for page in reader.pages:
            try:
                text = page.extract_text() or ""
            except Exception:
                continue

            for raw_line in text.splitlines():
                line = re.sub(r"\s+", " ", raw_line).strip()
                if not line:
                    continue

                upper = line.upper()
                if "FABRICATION" in upper and "MATERIAL" in upper:
                    section = "FABRICATION MATERIALS"
                    continue
                if "ERECTION" in upper and "MATERIAL" in upper:
                    section = "ERECTION MATERIALS"
                    continue

                if any(h in upper for h in ("DESCRIPTION", "COMMODITY", "QTY", "PT.", "CODE", "SIZE")):
                    continue

                if "FITTING" in upper and len(line.split()) <= 4:
                    category = "FITTINGS"
                    continue
                if "FLANGE" in upper and len(line.split()) <= 4:
                    category = "FLANGES"
                    continue
                if "BOLT" in upper and len(line.split()) <= 4:
                    category = "BOLTS"
                    continue
                if "VALVE" in upper and len(line.split()) <= 4:
                    category = "VALVES"
                    continue

                m = cls.ITEM_RE.match(line)
                if not m:
                    continue

                pt_no, middle, qty = m.group(1), m.group(2), m.group(3)
                rows.append(
                    MaterialRow(
                        section=section,
                        category=category,
                        pt_no=pt_no,
                        description=middle,
                        size_inch="",
                        commodity="",
                        code="",
                        qty=qty,
                    )
                )

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
    def _xml_safe_text(cls, value: str) -> str:
        # XLSX sheet XML only allows a restricted subset of Unicode code points.
        return INVALID_XML_CHARS_RE.sub("", value)

    @classmethod
    def _cell(cls, row_idx: int, col_idx: int, value: str) -> str:
        ref = f"{cls._col_name(col_idx)}{row_idx}"
        escaped = html.escape(cls._xml_safe_text(value))
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

    rows = MaterialTableParser(tokens).parse()
    if not rows:
        rows = PlainTextMaterialParser.parse_pdf(args.input_pdf)
    if not rows:
        raise SystemExit(
            "No material rows were detected. Try a clearer PDF or adjust parser thresholds."
        )

    XlsxWriter.write(rows, args.output)
    print(f"Extracted {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
