# PDF to Material Excel Extractor

This project extracts material table details from piping/material drawing PDFs and writes them to an Excel file (`.xlsx`).

## Requirements
- Python 3.9+
- Install dependencies:

```bash
pip3 install -r requirements.txt
```

## 1) CLI Usage

```bash
python3 "./extract_materials.py" \
  "/path/to/input.pdf" \
  -o "/path/to/output.xlsx"
```

Example:

```bash
python3 "./extract_materials.py" \
  "/Users/gokulkrishnan/Downloads/51408.pdf" \
  -o "/Users/gokulkrishnan/Documents/New project/materials_51408.xlsx"
```

## 2) Web UI Usage

Run:

```bash
python3 "./web_ui.py"
```

Open in browser:
- `http://127.0.0.1:8000`

Then:
- Upload a `.pdf`
- Click **Extract to Excel**
- The browser downloads `<filename>_materials.xlsx`

## Output Columns
- Section
- Category
- PT No
- Description
- Size (Inch)
- Commodity
- Code
- Qty

## Notes
- The parser is tuned for structured engineering material tables similar to the sample PDF.
- If another drawing format differs, column thresholds in `MaterialTableParser` may need adjustment in `/Users/gokulkrishnan/Documents/New project/extract_materials.py`.
