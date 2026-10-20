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


@pytest.mark.parametrize("message,wip", [
    ("q001(tsql): WIP attempt 2", True),
    ("wip", True),
    ("q001(tsql): Attempt 2", False),
    ("q001: done\n# comment lines mentioning wip are ignored", False),
])
def test_is_wip(message, wip):
    assert cli.is_wip(message) is wip


def test_wip_commit_is_not_graded(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_branch", lambda: "drill/me")
    monkeypatch.setattr(
        cli, "_git",
        lambda *a: "solutions/q003/answer.md" if a[0] == "diff" else "",
    )
    answer = cli.P.find("q003").solution_path(None)
    monkeypatch.setattr(type(answer), "is_file", lambda self: True)
    msg = tmp_path / "MSG"
    msg.write_text("q003: WIP first draft\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == 0
    assert "in progress" in capsys.readouterr().out


def test_solutions_off_drill_branches_are_refused(tmp_path, monkeypatch,
                                                  capsys):
    monkeypatch.setattr(cli, "_branch", lambda: "main")
    monkeypatch.setattr(cli, "suggested_branch", lambda: "drill/me")
    monkeypatch.setattr(
        cli, "_git",
        lambda *a: "solutions/q003/answer.md" if a[0] == "diff" else "",
    )
    monkeypatch.setattr(type(tmp_path), "is_file", lambda self: True)
    msg = tmp_path / "MSG"
    msg.write_text("feat: Add my answer\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == 1
    out = capsys.readouterr().out
    assert "drill/<username>" in out and "git switch -c drill/me" in out


def test_two_problems_in_one_commit_are_refused(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_branch", lambda: "drill/me")
    staged = "solutions/q003/answer.md\nsolutions/q006/answer.md"
    monkeypatch.setattr(
        cli, "_git", lambda *a: staged if a[0] == "diff" else ""
    )
    monkeypatch.setattr(type(tmp_path), "is_file", lambda self: True)
    msg = tmp_path / "MSG"
    msg.write_text("q003 and q006\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == 1
    assert "one problem per commit" in capsys.readouterr().out
