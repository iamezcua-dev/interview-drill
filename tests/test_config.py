from drill import config


def test_local_file_overrides_defaults_and_env_overrides_file(
    tmp_path, monkeypatch
):
    local = tmp_path / "drill.yaml"
    local.write_text("database:\n  port: 1432\n", encoding="utf-8")
    monkeypatch.setattr(config, "LOCAL_FILE", local)
    monkeypatch.delenv("DRILL_DB_PORT", raising=False)
    monkeypatch.setenv("DRILL_DB_PASSWORD", "x")
    config.reload()
    try:
        assert config.db_settings().port == 1432
        assert config.db_settings().host == "localhost"
        monkeypatch.setenv("DRILL_DB_PORT", "1500")
        config.reload()
        assert config.db_settings().port == 1500
    finally:
        config.reload()


def test_shell_sqlcmd_variables_are_ignored(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LOCAL_FILE", tmp_path / "missing.yaml")
    monkeypatch.setenv("SQLCMDSERVER", "elsewhere,9999")
    monkeypatch.setenv("DRILL_DB_PASSWORD", "x")
    config.reload()
    try:
        assert config.db_settings().port == 1433
    finally:
        config.reload()
