#!/usr/bin/env python3
from __future__ import annotations

import html
import os
import re
import tempfile
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from extract_materials import MaterialTableParser, PdfTextExtractor, XlsxWriter

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "10000"))
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


def _parse_boundary(content_type: str) -> bytes | None:
    m = re.search(r"boundary=([^;]+)", content_type, re.IGNORECASE)
    if not m:
        return None
    boundary = m.group(1).strip().strip('"')
    return boundary.encode("utf-8")


def _parse_multipart_file(content_type: str, body: bytes) -> tuple[str, bytes]:
    boundary = _parse_boundary(content_type)
    if not boundary:
        raise ValueError("Missing multipart boundary")

    marker = b"--" + boundary
    parts = body.split(marker)

    for raw_part in parts:
        part = raw_part.strip(b"\r\n")
        if not part or part == b"--":
            continue

        header_end = part.find(b"\r\n\r\n")
        if header_end < 0:
            continue

        header_blob = part[:header_end].decode("utf-8", errors="ignore")
        data = part[header_end + 4 :]

        # Remove final multipart terminator remnants.
        if data.endswith(b"\r\n"):
            data = data[:-2]

        if "name=\"pdf\"" not in header_blob:
            continue

        filename_match = re.search(r'filename="([^"]*)"', header_blob)
        filename = filename_match.group(1) if filename_match else "upload.pdf"
        return filename, data

    raise ValueError("No file field named 'pdf' found")


def _render_index(message: str = "", error: bool = False) -> str:
    msg_html = ""
    if message:
        cls = "notice error" if error else "notice"
        msg_html = f'<div class="{cls}">{html.escape(message)}</div>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PDF Material Extractor</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #f4f0e8;
      --ink: #1e2a39;
      --panel: #fffdf9;
      --accent: #c7461a;
      --accent-2: #0d8f7a;
      --line: #d7c9b3;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 5% 10%, #ffd39e 0%, transparent 36%),
        radial-gradient(circle at 90% 85%, #9de6d8 0%, transparent 38%),
        var(--bg);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    .card {{
      width: min(760px, 100%);
      background: var(--panel);
      border: 2px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 14px 40px rgba(36, 26, 15, 0.12);
      overflow: hidden;
      animation: enter .35s ease;
    }}
    @keyframes enter {{
      from {{ opacity: 0; transform: translateY(8px) scale(.99); }}
      to {{ opacity: 1; transform: translateY(0) scale(1); }}
    }}
    .head {{
      padding: 24px;
      border-bottom: 2px solid var(--line);
      background: linear-gradient(120deg, #fff4e1, #ebfbf8);
    }}
    h1 {{ margin: 0; font-size: clamp(1.4rem, 2vw + .8rem, 2.2rem); }}
    .sub {{ margin-top: 8px; opacity: .82; }}
    form {{ padding: 24px; display: grid; gap: 16px; }}
    .file {{
      border: 2px dashed var(--line);
      border-radius: 12px;
      padding: 22px;
      background: #fff;
    }}
    input[type="file"] {{
      width: 100%;
      font-family: "IBM Plex Mono", monospace;
      font-size: .94rem;
    }}
    button {{
      border: 0;
      border-radius: 12px;
      padding: 12px 16px;
      font-size: 1rem;
      font-weight: 700;
      font-family: inherit;
      color: #fff;
      background: linear-gradient(120deg, var(--accent), #ff7a3f);
      cursor: pointer;
      transition: transform .12s ease, box-shadow .12s ease;
    }}
    button:hover {{ transform: translateY(-1px); box-shadow: 0 8px 16px rgba(199, 70, 26, .22); }}
    .meta {{
      margin-top: 4px;
      font-size: .86rem;
      opacity: .78;
      font-family: "IBM Plex Mono", monospace;
    }}
    .notice {{
      margin: 16px 24px 0;
      border-radius: 10px;
      border: 1px solid #9cd9cc;
      background: #ecfffa;
      color: #0f5b4c;
      padding: 10px 12px;
      font-size: .94rem;
    }}
    .error {{
      border-color: #efb7ab;
      background: #fff0ec;
      color: #7a2d1d;
    }}
  </style>
</head>
<body>
  <main class="card">
    <section class="head">
      <h1>PDF Material Extractor</h1>
      <div class="sub">Upload drawing PDF -> get material details as Excel (.xlsx)</div>
    </section>
    {msg_html}
    <form method="post" action="/extract" enctype="multipart/form-data">
      <label class="file">
        <input type="file" name="pdf" accept=".pdf,application/pdf" required />
        <div class="meta">Max upload: {MAX_UPLOAD_BYTES // (1024 * 1024)} MB</div>
      </label>
      <button type="submit">Extract to Excel</button>
    </form>
  </main>
</body>
</html>
"""


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", ""):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        body = _render_index().encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/extract":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0:
                raise ValueError("Empty request body")
            if content_length > MAX_UPLOAD_BYTES:
                raise ValueError(f"File too large. Max allowed is {MAX_UPLOAD_BYTES // (1024 * 1024)} MB")

            content_type = self.headers.get("Content-Type", "")
            if "multipart/form-data" not in content_type.lower():
                raise ValueError("Request must be multipart/form-data")

            body = self.rfile.read(content_length)
            filename, pdf_data = _parse_multipart_file(content_type, body)
            if not filename.lower().endswith(".pdf"):
                raise ValueError("Please upload a .pdf file")
            if not pdf_data.startswith(b"%PDF"):
                raise ValueError("Uploaded file is not a valid PDF")

            with tempfile.TemporaryDirectory(prefix="pdf_extract_") as td:
                tmp_dir = Path(td)
                input_pdf = tmp_dir / "input.pdf"
                input_pdf.write_bytes(pdf_data)

                tokens = PdfTextExtractor(input_pdf).extract_tokens()
                if not tokens:
                    raise ValueError("Could not extract text from PDF")

                rows = MaterialTableParser(tokens).parse()
                if not rows:
                    raise ValueError("No material rows found in this PDF")

                out_xlsx = tmp_dir / "materials.xlsx"
                XlsxWriter.write(rows, out_xlsx)
                xlsx_bytes = out_xlsx.read_bytes()

            base_name = Path(filename).stem
            download_name = f"{base_name}_materials.xlsx"

            self.send_response(HTTPStatus.OK)
            self.send_header(
                "Content-Type",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            self.send_header("Content-Length", str(len(xlsx_bytes)))
            self.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
            self.end_headers()
            self.wfile.write(xlsx_bytes)

        except Exception as exc:
            body = _render_index(str(exc), error=True).encode("utf-8")
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        return


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), AppHandler)
    print(f"Web UI running at http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
