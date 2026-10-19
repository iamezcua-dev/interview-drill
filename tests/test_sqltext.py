from drill.sqltext import is_blank, spark_statements, tsql_batches


def test_tsql_go_batches_and_comment_only_file():
    text = "SELECT 1\nGO\n-- only a comment\nGO 2\nSELECT 2;\n"
    assert tsql_batches(text) == ["SELECT 1", "SELECT 2;"]
    assert tsql_batches("-- q001 stub\n/* nothing */\n") == []


def test_spark_statements_respect_strings_and_comments():
    text = "SELECT ';' AS x; -- a; b\nSELECT 2 /* ; */;\n-- end"
    assert spark_statements(text) == [
        "SELECT ';' AS x", "-- a; b\nSELECT 2 /* ; */"
    ]


def test_is_blank():
    assert is_blank("  -- hi\n ; ")
    assert not is_blank("SELECT 1")
