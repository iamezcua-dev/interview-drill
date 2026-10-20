"""Split SQL text without needing sqlcmd.

* T-SQL: batches separated by `GO` lines (a sqlcmd convention, not SQL).
* Spark SQL: statements separated by `;` outside strings and comments.
"""

import re

_GO = re.compile(r"^\s*go(\s+\d+)?\s*;?\s*$", re.IGNORECASE)


def strip_comments(sql: str) -> str:
    out, i, n = [], 0, len(sql)
    while i < n:
        two = sql[i:i + 2]
        if two == "--":
            end = sql.find("\n", i)
            i = n if end == -1 else end
        elif two == "/*":
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
        elif sql[i] in "'\"`":
            quote, j = sql[i], i + 1
            while j < n:
                if sql[j] == quote:
                    if j + 1 < n and sql[j + 1] == quote:
                        j += 2
                        continue
                    break
                j += 1
            out.append(sql[i:j + 1])
            i = j + 1
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def is_blank(sql: str) -> bool:
    return not strip_comments(sql).strip(" \t\r\n;")


def tsql_batches(text: str) -> list[str]:
    batches, current = [], []
    for line in text.splitlines():
        if _GO.match(line):
            batches.append("\n".join(current))
            current = []
        else:
            current.append(line)
    batches.append("\n".join(current))
    return [b for b in batches if not is_blank(b)]


def spark_statements(text: str) -> list[str]:
    statements, current, i, n = [], [], 0, len(text)
    while i < n:
        two = text[i:i + 2]
        if two == "--":
            end = text.find("\n", i)
            end = n if end == -1 else end
            current.append(text[i:end])
            i = end
        elif two == "/*":
            end = text.find("*/", i + 2)
            end = n if end == -1 else end + 2
            current.append(text[i:end])
            i = end
        elif text[i] in "'\"`":
            quote, j = text[i], i + 1
            while j < n and text[j] != quote:
                j += 2 if text[j] == "\\" else 1
            current.append(text[i:j + 1])
            i = j + 1
        elif text[i] == ";":
            statements.append("".join(current))
            current = []
            i += 1
        else:
            current.append(text[i])
            i += 1
    statements.append("".join(current))
    return [s.strip() for s in statements if not is_blank(s)]
