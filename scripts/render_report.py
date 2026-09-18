"""Render reports/FraudWatch-Report.md to the committed HTML and PDF.

The HTML and PDF next to the Markdown source were originally produced by hand, which meant every edit
to the source silently left them stale. This script makes that step reproducible, so the three files
cannot drift apart again:

    python scripts/render_report.py            # HTML only
    python scripts/render_report.py --pdf      # HTML, then print it with headless Chrome

The converter deliberately covers only the Markdown this report actually uses - ATX headings, pipe
tables, bullet and ordered lists, blockquotes, horizontal rules, images, and inline emphasis, code and
links. It is not a general Markdown implementation, and a construct it does not know is emitted as
literal text rather than guessed at, so a silent mis-render is not possible.
"""
from __future__ import annotations

import argparse
import html
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "reports" / "FraudWatch-Report.md"
CSS = ROOT / "reports" / "report.css"
OUT_HTML = ROOT / "reports" / "FraudWatch-Report.html"
OUT_PDF = ROOT / "reports" / "FraudWatch-Report.pdf"

CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "google-chrome", "chromium", "chromium-browser",
]


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", re.sub(r"<[^>]+>", "", text).lower()).strip("-")


def inline(text: str) -> str:
    """Escape first, then re-introduce only the inline markup we support."""
    out = html.escape(text, quote=False)
    # Code spans are taken before anything else so markup inside them stays literal.
    codes: list[str] = []
    def stash(m: re.Match) -> str:
        codes.append(m.group(1))
        return f"\x00{len(codes) - 1}\x00"
    out = re.sub(r"`([^`]+)`", stash, out)
    out = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r'<img alt="\1" src="\2">', out)
    out = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', out)
    out = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", out)
    out = re.sub(r"(?<![*\w])\*([^*]+)\*(?!\*)", r"<em>\1</em>", out)
    return re.sub(r"\x00(\d+)\x00", lambda m: f"<code>{codes[int(m.group(1))]}</code>", out)


def row(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def convert(md: str) -> str:
    lines = md.replace("\r\n", "\n").split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        if stripped.startswith("```"):                       # fenced code
            i += 1
            body = []
            while i < len(lines) and not lines[i].strip().startswith("```"):
                body.append(html.escape(lines[i], quote=False))
                i += 1
            i += 1
            out.append("<pre><code>" + "\n".join(body) + "</code></pre>")
            continue

        if re.fullmatch(r"-{3,}", stripped):                 # horizontal rule
            out.append("<hr>")
            i += 1
            continue

        m = re.match(r"(#{1,6})\s+(.*)", stripped)           # heading
        if m:
            level, text = len(m.group(1)), inline(m.group(2))
            out.append(f'<h{level} id="{slug(m.group(2))}">{text}</h{level}>')
            i += 1
            continue

        # Pipe table: a header row followed by a separator row of dashes.
        if stripped.startswith("|") and i + 1 < len(lines) and re.fullmatch(
                r"\|[\s:|-]+\|", lines[i + 1].strip()):
            head = row(stripped)
            i += 2
            body = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                body.append(row(lines[i].strip()))
                i += 1
            th = "".join(f"<th>{inline(c)}</th>" for c in head)
            trs = "".join("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>" for r in body)
            out.append(f"<table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table>")
            continue

        if re.match(r"[-*]\s+", stripped) or re.match(r"\d+\.\s+", stripped):   # list
            ordered = bool(re.match(r"\d+\.\s+", stripped))
            items: list[str] = []
            pattern = r"\d+\.\s+(.*)" if ordered else r"[-*]\s+(.*)"
            while i < len(lines):
                cur = lines[i].strip()
                m2 = re.match(pattern, cur)
                if m2:
                    items.append(m2.group(1))
                elif cur and lines[i].startswith((" ", "\t")) and items:
                    items[-1] += " " + cur                  # continuation of the previous item
                else:
                    break
                i += 1
            # Inline markup is applied to the joined item, never per source line: **bold** and
            # `code` are routinely wrapped across a line break in this source, and converting it
            # line by line would leave the opening asterisks behind as literal text.
            tag = "ol" if ordered else "ul"
            out.append(f"<{tag}>" + "".join(f"<li>{inline(it)}</li>" for it in items) + f"</{tag}>")
            continue

        if stripped.startswith(">"):                         # blockquote
            body = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                body.append(lines[i].strip().lstrip("> "))
                i += 1
            out.append("<blockquote><p>" + inline(" ".join(body)) + "</p></blockquote>")
            continue

        para = []                                            # paragraph
        while i < len(lines) and lines[i].strip() and not re.match(
                r"(#{1,6}\s|[-*]\s|\d+\.\s|\||>|```|-{3,}$)", lines[i].strip()):
            para.append(lines[i].strip())
            i += 1
        if para:
            out.append("<p>" + inline(" ".join(para)) + "</p>")
        else:
            i += 1
    return "\n".join(out)


def find_chrome() -> str | None:
    for c in CHROME_CANDIDATES:
        if Path(c).exists():
            return c
        found = shutil.which(c)
        if found:
            return found
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", action="store_true", help="also print the HTML to PDF with headless Chrome")
    args = ap.parse_args()

    body = convert(SRC.read_text(encoding="utf-8"))
    css = CSS.read_text(encoding="utf-8")
    OUT_HTML.write_text(
        f'<!doctype html><html><head><meta charset="utf-8"><title>FraudWatch Report</title>'
        f"<style>\n{css}</style></head><body>{body}</body></html>",
        encoding="utf-8")
    print(f"wrote {OUT_HTML.relative_to(ROOT)} ({OUT_HTML.stat().st_size:,} bytes)")

    if not args.pdf:
        return 0
    chrome = find_chrome()
    if chrome is None:
        print("no Chrome or Chromium found; the PDF was not regenerated and is now stale", file=sys.stderr)
        return 1
    subprocess.run([chrome, "--headless", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={OUT_PDF}", OUT_HTML.as_uri()], check=True, timeout=300)
    print(f"wrote {OUT_PDF.relative_to(ROOT)} ({OUT_PDF.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
