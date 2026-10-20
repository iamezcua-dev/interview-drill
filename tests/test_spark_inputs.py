from drill.adapters.spark import _literal, _table_sql
from drill.problems import Column, Table


def test_literals_are_typed_and_escaped():
    assert _literal("string(10)", "O'Brien\\x") == "'O\\'Brien\\\\x'"
    assert _literal("decimal(10,2)", 800) == "CAST('800' AS DECIMAL(10,2))"
    assert _literal("date", "2024-01-31") == "DATE'2024-01-31'"
    assert _literal("int", None) == "CAST(NULL AS INT)"


def test_empty_table_keeps_its_schema():
    table = Table("t", (Column("a", "int"), Column("b", "string(5)")))
    sql = _table_sql(table, [])
    assert sql.endswith("WHERE false")
    assert "AS `a`" in sql and "AS `b`" in sql
