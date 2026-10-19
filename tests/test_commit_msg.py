import pytest

from drill import cli


@pytest.mark.parametrize("branch,subject,code", [
    ("feature/x", "feat(q016): Add running total problem", 0),
    ("feature/x", "fix: Correct q002 tied-date case", 0),
    ("feature/x", "Q15 - SQL", 1),
    ("drill/eddiejobs", "q001 attempt 2", 0),
    ("feature/x", "Merge branch 'main' into feature/x", 0),
])
def test_commit_msg(tmp_path, monkeypatch, branch, subject, code):
    monkeypatch.setattr(cli, "_branch", lambda: branch)
    msg = tmp_path / "MSG"
    msg.write_text(subject + "\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == code
