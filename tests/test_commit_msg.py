import pytest

from drill import cli


@pytest.mark.parametrize("branch,subject,code", [
    ("feature/x", "feat(k7q2): Add running total problem", 0),
    ("feature/x", "fix: Correct b7x9ye tied-date case", 0),
    ("feature/x", "Q15 - SQL", 1),
    ("drill/eddiejobs", "7xm8xp attempt 2", 0),
    ("feature/x", "Merge branch 'main' into feature/x", 0),
])
def test_commit_msg(tmp_path, monkeypatch, branch, subject, code):
    monkeypatch.setattr(cli, "_branch", lambda: branch)
    msg = tmp_path / "MSG"
    msg.write_text(subject + "\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == code


@pytest.mark.parametrize("message,wip", [
    ("7xm8xp(tsql): WIP attempt 2", True),
    ("wip", True),
    ("7xm8xp(tsql): Attempt 2", False),
    ("7xm8xp: done\n# comment lines mentioning wip are ignored", False),
])
def test_is_wip(message, wip):
    assert cli.is_wip(message) is wip


def test_wip_commit_is_not_graded(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_branch", lambda: "drill/me")
    monkeypatch.setattr(
        cli, "_git",
        lambda *a: (
            "solutions/7xm8xp/solution.pyspark.py" if a[0] == "diff" else ""
        ),
    )
    answer = cli.P.find("7xm8xp").solution_path("pyspark")
    monkeypatch.setattr(type(answer), "is_file", lambda self: True)
    msg = tmp_path / "MSG"
    msg.write_text("7xm8xp(pyspark): WIP first draft\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == 0
    assert "in progress" in capsys.readouterr().out


def test_solutions_off_drill_branches_are_refused(tmp_path, monkeypatch,
                                                  capsys):
    monkeypatch.setattr(cli, "_branch", lambda: "main")
    monkeypatch.setattr(cli, "suggested_branch", lambda: "drill/me")
    monkeypatch.setattr(
        cli, "_git",
        lambda *a: (
            "solutions/7xm8xp/solution.pyspark.py" if a[0] == "diff" else ""
        ),
    )
    monkeypatch.setattr(type(tmp_path), "is_file", lambda self: True)
    msg = tmp_path / "MSG"
    msg.write_text("feat: Add my answer\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == 1
    out = capsys.readouterr().out
    assert "drill/<username>" in out and "git switch -c drill/me" in out


def test_two_problems_in_one_commit_are_refused(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_branch", lambda: "drill/me")
    staged = ("solutions/7xm8xp/solution.pyspark.py\n"
              "solutions/b7x9ye/solution.pyspark.py")
    monkeypatch.setattr(
        cli, "_git", lambda *a: staged if a[0] == "diff" else ""
    )
    monkeypatch.setattr(type(tmp_path), "is_file", lambda self: True)
    msg = tmp_path / "MSG"
    msg.write_text("7xm8xp and b7x9ye\n", encoding="utf-8")
    assert cli.main(["commit-msg", str(msg)]) == 1
    assert "one problem per commit" in capsys.readouterr().out
