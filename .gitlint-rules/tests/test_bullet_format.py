"""Tests for the BulletFormatRule custom gitlint rule (UC1)."""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def run_gitlint(message: str) -> tuple[int, str]:
    """Pipe a commit message into gitlint and return (exit_code, output)."""
    result = subprocess.run(
        ["gitlint"],
        input=message,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return result.returncode, result.stdout + result.stderr


def make_message(*body_lines: str) -> str:
    """Build a commit message with a valid title plus the given body lines."""
    title = "feat: Add something meaningful"
    return title + "\n\n" + "\n".join(body_lines) + "\n"


# --- Happy path ---------------------------------------------------------------


def test_single_well_formed_bullet_passes():
    code, _ = run_gitlint(make_message("- Adds a feature and ends with a period."))
    assert code == 0


def test_multiline_bullet_two_space_continuation_passes():
    code, _ = run_gitlint(
        make_message(
            "- Adds a feature whose description wraps onto",
            "  the next continuation line with a period.",
        )
    )
    assert code == 0


def test_multiple_bullets_pass():
    code, _ = run_gitlint(
        make_message(
            "- First bullet ends well.",
            "- Second bullet also ends well.",
        )
    )
    assert code == 0


# --- Uppercase rule -----------------------------------------------------------


def test_lowercase_start_fails_with_uc1():
    code, out = run_gitlint(make_message("- lowercase start fails."))
    assert code != 0
    assert "UC1" in out
    assert "uppercase" in out.lower()


# --- Period rule --------------------------------------------------------------


def test_missing_period_fails_with_uc1():
    code, out = run_gitlint(make_message("- Missing trailing period"))
    assert code != 0
    assert "UC1" in out
    assert "period" in out.lower()


def test_missing_period_on_last_continuation_line_fails():
    code, out = run_gitlint(
        make_message(
            "- Wrapped bullet whose last line has",
            "  no period at the end",
        )
    )
    assert code != 0
    assert "UC1" in out


# --- Mixed bullets ------------------------------------------------------------


def test_only_offending_bullet_is_flagged():
    code, out = run_gitlint(
        make_message(
            "- Good first bullet.",
            "- bad second bullet",
        )
    )
    assert code != 0
    # Good bullet on body line 1 should not be reported; bad one on line 2 should.
    assert "bad second bullet" in out
    assert "Good first bullet" not in out
