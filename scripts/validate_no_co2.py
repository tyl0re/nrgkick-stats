#!/usr/bin/env python3
"""Quick validation: ensure CO2 is not present in HTML report or code.

Returns:
- 0 when OK (no CO2 in HTML and no CO2 references in code)
- 1 when CO2 is present in HTML
- 2 when CO2 is referenced in code
"""
import os
import re
import glob


def _find_report_content():
    # Try environment overrides first
    for key in ("NRG_TEST_REPORT", "NRG_REPORT"):
        path = os.environ.get(key)
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
    # Common locations in project layout
    candidates = [
        os.path.join("reports", "latest.html"),
        "latest.html",
    ]
    # Also try LOCALAPPDATA path if available
    local = os.environ.get("LOCALAPPDATA")
    if local:
        candidates.append(os.path.join(local, "NRGkickLogger", "reports", "latest.html"))

    for c in candidates:
        if c and os.path.exists(c):
            with open(c, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
    return None


def _html_contains_co2(content: str | None) -> bool:
    if not content:
        return False
    return bool(re.search(r"\bCO2\b|CO2 geschaetzt|CO2 kg", content, re.IGNORECASE))


def _code_contains_co2():
    bad_paths = []
    for path in glob.glob("**/*.py", recursive=True):
        # skip the validator itself to avoid false positives
        if path.endswith("validate_no_co2.py"):
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                s = f.read()
        except Exception:
            continue
        if re.search(r"\bCO2\b|co2_kg|costs\..*co2|co2_g_per_kwh", s, re.IGNORECASE):
            bad_paths.append(path)
    return bad_paths


def main() -> int:
    html = _find_report_content()
    html_ok = not _html_contains_co2(html)
    code_issues = _code_contains_co2()

    print(f"HTML CO2 present: {not html_ok}")
    print(f"Code CO2 references found: {len(code_issues)}")
    if not html_ok:
        print(" - CO2 found in HTML report.")
        return 1
    if code_issues:
        print(" - CO2 references found in code:")
        for p in code_issues:
            print(f"   - {p}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
