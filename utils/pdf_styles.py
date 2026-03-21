"""
pdf_styles.py — Shared PDF utilities for SV Fincloud ERP
All PDF reports use these helpers to ensure a consistent, professional layout.
"""
import io
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

# ── Font names (registered in server.py at startup) ──────────────────────────
FONT_REGULAR = "DejaVu"
FONT_BOLD    = "DejaVu-Bold"

# ── Brand colours ─────────────────────────────────────────────────────────────
COLOR_BRAND      = colors.HexColor("#1e3a8a")   # dark blue — header / table bg
COLOR_BRAND_MID  = colors.HexColor("#2563eb")   # mid blue  — divider / accents
COLOR_TOTAL_BG   = colors.HexColor("#f0f4ff")   # light blue — TOTAL row bg
COLOR_TOTAL_LINE = colors.HexColor("#2563eb")   # TOTAL row top border
COLOR_ROW_ALT    = colors.HexColor("#f8fafc")   # alternating row tint
COLOR_GRID       = colors.HexColor("#e2e8f0")   # table grid lines
COLOR_FOOTER     = colors.HexColor("#94a3b8")   # footer text
COLOR_FOOTER_LINE= colors.HexColor("#e2e8f0")   # footer separator

# ── Page geometry ─────────────────────────────────────────────────────────────
PAGE_MARGIN   = 50          # left / right margin (points)
PAGE_W, PAGE_H = A4


# ─────────────────────────────────────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────────────────────────────────────

def fmt_currency(amount) -> str:
    """₹1,23,456  (no decimals)"""
    try:
        return f"\u20b9{int(round(float(amount))):,}"
    except (TypeError, ValueError):
        return "-"


def fmt_datetime(ts) -> str:
    """16/03/2026 06:10 PM"""
    if ts is None:
        return "-"
    if hasattr(ts, "strftime"):
        return ts.strftime("%d/%m/%Y %I:%M %p")
    try:
        from dateutil import parser as dtparser
        return dtparser.parse(str(ts)).strftime("%d/%m/%Y %I:%M %p")
    except Exception:
        return str(ts)[:16]


def fmt_date(ts) -> str:
    """16/03/2026"""
    if ts is None:
        return "-"
    if hasattr(ts, "strftime"):
        return ts.strftime("%d/%m/%Y")
    try:
        from dateutil import parser as dtparser
        return dtparser.parse(str(ts)).strftime("%d/%m/%Y")
    except Exception:
        return str(ts)[:10]


# ─────────────────────────────────────────────────────────────────────────────
# Paragraph styles
# ─────────────────────────────────────────────────────────────────────────────

def _style(name, **kwargs) -> ParagraphStyle:
    defaults = dict(fontName=FONT_REGULAR, spaceAfter=4)
    defaults.update(kwargs)
    return ParagraphStyle(name, **defaults)


STYLE_COMPANY = _style(
    "PDFCompany",
    fontSize=20, alignment=1,
    spaceAfter=4, textColor=COLOR_BRAND,
    leading=24, fontName=FONT_BOLD,
)
STYLE_BRANCH = _style(
    "PDFBranch",
    fontSize=12, alignment=1,
    spaceAfter=3, textColor=colors.HexColor("#334155"),
)
STYLE_PERIOD = _style(
    "PDFPeriod",
    fontSize=10, alignment=1,
    spaceAfter=12, textColor=colors.HexColor("#64748b"),
)
STYLE_INFO = _style(
    "PDFInfo",
    fontSize=10, alignment=0,
    spaceAfter=4, textColor=colors.HexColor("#334155"),
)
STYLE_TITLE = _style(
    "PDFTitle",
    fontSize=13, alignment=0,
    spaceAfter=6, textColor=COLOR_BRAND,
    fontName=FONT_BOLD,
)
STYLE_CELL = _style(
    "PDFCell",
    fontSize=9, alignment=1,
    spaceAfter=0, leading=12,
)
STYLE_CELL_BOLD = _style(
    "PDFCellBold",
    fontSize=9, alignment=1,
    spaceAfter=0, leading=12,
    fontName=FONT_BOLD,
)


# ─────────────────────────────────────────────────────────────────────────────
# Header builder
# ─────────────────────────────────────────────────────────────────────────────

def build_header(company_name: str, branch_name: str, period: str = None) -> list:
    """
    Returns a list of flowables for the standard report header:
      Company Name (bold, blue, centered)
      Branch Name  (smaller, centered)
      Period line  (grey, centered)  — omitted if period is None
      Blue divider line
      Spacer
    """
    elems = [
        Spacer(1, 6),
        Paragraph(company_name, STYLE_COMPANY),
        Paragraph(branch_name,  STYLE_BRANCH),
    ]
    if period:
        elems.append(Paragraph(f"Period: {period}", STYLE_PERIOD))
    elems += [
        HRFlowable(width="100%", thickness=1.5, color=COLOR_BRAND_MID),
        Spacer(1, 10),
    ]
    return elems


# ─────────────────────────────────────────────────────────────────────────────
# Table style builders
# ─────────────────────────────────────────────────────────────────────────────

def build_table_style(total_row_idx: int = None) -> TableStyle:
    """
    Standard table style.
    Pass total_row_idx (0-based index of the TOTAL row) to apply bold/highlight.
    """
    cmds = [
        # Header
        ("BACKGROUND",    (0, 0), (-1, 0), COLOR_BRAND),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), FONT_BOLD),
        ("FONTSIZE",      (0, 0), (-1, 0), 9),
        ("ALIGN",         (0, 0), (-1, 0), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING",    (0, 0), (-1, 0), 8),
        # Body
        ("FONTNAME",      (0, 1), (-1, -1), FONT_REGULAR),
        ("FONTSIZE",      (0, 1), (-1, -1), 9),
        ("ALIGN",         (0, 1), (-1, -1), "CENTER"),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
        ("TOPPADDING",    (0, 1), (-1, -1), 6),
        ("GRID",          (0, 0), (-1, -1), 0.4, COLOR_GRID),
    ]

    # Alternating rows (exclude total row if present)
    alt_end = (-1, total_row_idx - 1) if total_row_idx else (-1, -1)
    cmds.append(("ROWBACKGROUNDS", (0, 1), alt_end, [colors.white, COLOR_ROW_ALT]))

    # TOTAL row
    if total_row_idx is not None:
        cmds += [
            ("BACKGROUND",    (0, total_row_idx), (-1, total_row_idx), COLOR_TOTAL_BG),
            ("FONTNAME",      (0, total_row_idx), (-1, total_row_idx), FONT_BOLD),
            ("FONTSIZE",      (0, total_row_idx), (-1, total_row_idx), 10),
            ("LINEABOVE",     (0, total_row_idx), (-1, total_row_idx), 1.5, COLOR_TOTAL_LINE),
            ("BOTTOMPADDING", (0, total_row_idx), (-1, total_row_idx), 8),
            ("TOPPADDING",    (0, total_row_idx), (-1, total_row_idx), 8),
        ]

    return TableStyle(cmds)


# ─────────────────────────────────────────────────────────────────────────────
# Footer callback
# ─────────────────────────────────────────────────────────────────────────────

def make_footer_cb(company_name: str = ""):
    """Returns an onPage callback that draws the standard footer."""
    def _footer(canvas, doc):
        width, height = doc.pagesize
        width = float(width)
        margin = 50
        canvas.saveState()
        canvas.setStrokeColor(COLOR_FOOTER_LINE)
        canvas.line(margin, 42, width - margin, 42)
        canvas.setFont(FONT_REGULAR, 8)
        canvas.setFillColor(COLOR_FOOTER)
        canvas.drawString(margin, 28,
                          f"Generated: {datetime.now().strftime('%d/%m/%Y %I:%M %p')}")
        canvas.drawRightString(width - margin, 28,
                               f"Page {canvas.getPageNumber()}")
        canvas.restoreState()
    return _footer


# ─────────────────────────────────────────────────────────────────────────────
# Doc builder
# ─────────────────────────────────────────────────────────────────────────────

def make_doc(buffer, margin: int = PAGE_MARGIN) -> SimpleDocTemplate:
    """Standard A4 SimpleDocTemplate with consistent margins."""
    return SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=margin, leftMargin=margin,
        topMargin=40, bottomMargin=52,
    )


# ─────────────────────────────────────────────────────────────────────────────
# High-level helper: build a complete standard PDF
# ─────────────────────────────────────────────────────────────────────────────

def build_standard_pdf(
    company_name: str,
    branch_name: str,
    period: str,
    title: str,
    summary_items: list,   # list of (label, value) tuples
    table_headers: list,
    table_rows: list,
    col_widths: list = None,
    has_total_row: bool = False,
) -> io.BytesIO:
    """
    Build a complete standard PDF and return a BytesIO buffer.

    summary_items: e.g. [("Total Records", "42"), ("Total Amount", "₹1,23,456")]
    has_total_row: if True, the last row in table_rows is styled as a TOTAL row.
    """
    buffer = io.BytesIO()
    doc = make_doc(buffer)
    elements = []

    # Header
    elements.extend(build_header(company_name, branch_name, period))

    # Title
    if title:
        elements.append(Paragraph(title, STYLE_TITLE))
        elements.append(Spacer(1, 4))

    # Summary
    for label, value in summary_items:
        elements.append(Paragraph(f"<b>{label}:</b>  {value}", STYLE_INFO))
    if summary_items:
        elements.append(Spacer(1, 14))

    # Table
    usable_w = PAGE_W - 2 * PAGE_MARGIN
    if col_widths is None:
        n = len(table_headers)
        col_widths = [usable_w / n] * n

    def _wrap(cell):
        if isinstance(cell, str):
            return Paragraph(cell, STYLE_CELL)
        return cell

    wrapped_headers = [Paragraph(h, STYLE_CELL_BOLD) for h in table_headers]
    wrapped_rows = [[_wrap(cell) for cell in row] for row in table_rows]

    data = [wrapped_headers] + wrapped_rows
    total_row_idx = len(data) - 1 if has_total_row else None

    tbl = Table(data, colWidths=col_widths, hAlign="CENTER", repeatRows=1)
    tbl.setStyle(build_table_style(total_row_idx))
    elements.append(tbl)

    footer_cb = make_footer_cb()
    doc.build(elements, onFirstPage=footer_cb, onLaterPages=footer_cb)
    buffer.seek(0)
    return buffer
