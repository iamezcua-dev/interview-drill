"""Load problems: frontmatter, table schemas and language-neutral cases."""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from drill import LANGUAGES, SOLUTION_FILES, THEORY_FILE
from drill.config import PROBLEMS_DIR, SOLUTIONS_DIR

# Problem ids are short random codes (e.g. "k7q2"): unique across forks
# without coordination, and stable forever. No look-alike characters.
ID_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz"
ID_LENGTH = 4
ID_RE = re.compile(rf"^[{ID_ALPHABET}]{{{ID_LENGTH}}}$")
FOLDER_RE = re.compile(
    rf"^([{ID_ALPHABET}]{{{ID_LENGTH}}})-[a-z0-9]+(?:-[a-z0-9]+)*$"
)
TYPE_RE = re.compile(
    r"^(int|bigint|double|date|timestamp|boolean|string(\(\d+\))?"
    r"|decimal\(\d+,\s*\d+\))$"
)


class ProblemError(Exception):
    """A problem folder is malformed (an author error, not a solver one)."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = False


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]


@dataclass(frozen=True)
class Case:
    name: str
    path: Path
    input: dict[str, list[list[Any]]]
    rows: list[list[Any]] | None
    asserts: list[dict[str, str]]
    sorted_by: list[tuple[str, bool]]


@dataclass
class Problem:
    id: str
    slug: str
    path: Path
    title: str
    type: str
    level: str
    topic: str | None
    languages: tuple[str, ...]
    output: tuple[str, ...]
    tolerance: float
    sorted_by: list[tuple[str, bool]]
    statement: str
    tables: tuple[Table, ...] = field(default_factory=tuple)

    @property
    def is_theory(self) -> bool:
        return self.type == "theory"

    def solution_dir(self) -> Path:
        return SOLUTIONS_DIR / self.id

    def solution_path(self, lang: str | None) -> Path:
        name = THEORY_FILE if self.is_theory else SOLUTION_FILES[lang]
        return self.solution_dir() / name

    def stub_path(self, lang: str | None) -> Path:
        name = THEORY_FILE if self.is_theory else SOLUTION_FILES[lang]
        return self.path / "stubs" / name

    def reference_path(self, lang: str | None) -> Path:
        name = THEORY_FILE if self.is_theory else SOLUTION_FILES[lang]
        return self.path / "reference" / name

    def table(self, name: str) -> Table:
        for t in self.tables:
            if t.name == name:
                return t
        raise ProblemError(f"{self.id}: unknown table '{name}'")

    def cases(self) -> list[Case]:
        files = sorted((self.path / "cases").glob("*.yaml"))
        if not files:
            raise ProblemError(f"{self.id}: no cases/*.yaml found")
        return [self._load_case(f) for f in files]

    def _load_case(self, path: Path) -> Case:
        data = _read_yaml(path)
        where = f"{self.id}/{path.name}"
        inputs = data.get("input") or {}
        for name, rows in inputs.items():
            table = self.table(name)
            for row in rows or []:
                if len(row) != len(table.columns):
                    raise ProblemError(
                        f"{where}: row {row} in '{name}' has {len(row)} "
                        f"values, expected {len(table.columns)}"
                    )
        expected = data.get("expected") or {}
        rows = expected.get("rows")
        for row in rows or []:
            if len(row) != len(self.output):
                raise ProblemError(
                    f"{where}: expected row {row} has {len(row)} values, "
                    f"expected {len(self.output)}"
                )
        asserts = expected.get("asserts") or []
        if rows is None and not asserts:
            raise ProblemError(f"{where}: needs expected.rows or asserts")
        sorted_by = _sort_keys(expected.get("sorted_by")) or self.sorted_by
        return Case(
            name=data.get("name") or path.stem,
            path=path,
            input={k: list(v or []) for k, v in inputs.items()},
            rows=rows,
            asserts=asserts,
            sorted_by=sorted_by,
        )


def _read_yaml(path: Path) -> dict:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ProblemError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise ProblemError(f"{path}: expected a mapping at the top level")
    return data


def _sort_keys(raw: Any) -> list[tuple[str, bool]]:
    """`[{total_spent: desc}, customer_id]` -> [(col, descending), ...]."""
    keys = []
    for item in raw or []:
        if isinstance(item, str):
            keys.append((item, False))
        else:
            ((col, direction),) = item.items()
            keys.append((col, str(direction).lower() == "desc"))
    return keys


def _split_frontmatter(text: str, path: Path) -> tuple[dict, str]:
    if not text.startswith("---"):
        raise ProblemError(f"{path}: missing YAML frontmatter")
    _, front, body = text.split("---", 2)
    meta = yaml.safe_load(front) or {}
    return meta, body.strip()


def _load_tables(path: Path) -> tuple[Table, ...]:
    file = path / "tables.yaml"
    if not file.is_file():
        raise ProblemError(f"{path.name}: missing tables.yaml")
    tables = []
    for name, spec in _read_yaml(file).items():
        columns = []
        for col in spec["columns"]:
            ((col_name, col_type),) = (
                (k, v) for k, v in col.items() if k != "nullable"
            )
            col_type = str(col_type).lower().replace(" ", "")
            if not TYPE_RE.match(col_type):
                raise ProblemError(
                    f"{path.name}/tables.yaml: unsupported type "
                    f"'{col_type}' for {name}.{col_name}"
                )
            columns.append(
                Column(col_name, col_type, bool(col.get("nullable", False)))
            )
        tables.append(Table(name, tuple(columns)))
    return tuple(tables)


def load(path: Path) -> Problem:
    match = FOLDER_RE.match(path.name)
    if not match:
        raise ProblemError(
            f"{path.name}: folder must be named q<NNN>-<kebab-slug>"
        )
    pid = match.group(1)
    meta, statement = _split_frontmatter(
        (path / "problem.md").read_text(encoding="utf-8"), path
    )
    if meta.get("id") != pid:
        raise ProblemError(f"{path.name}: frontmatter id must be '{pid}'")
    ptype = meta.get("type")
    if ptype not in ("coding", "theory"):
        raise ProblemError(f"{pid}: type must be 'coding' or 'theory'")
    languages = tuple(meta.get("languages") or ())
    unknown = set(languages) - set(LANGUAGES)
    if unknown:
        raise ProblemError(f"{pid}: unknown languages {sorted(unknown)}")
    if ptype == "coding" and not languages:
        raise ProblemError(f"{pid}: coding problems need 'languages'")
    problem = Problem(
        id=pid,
        slug=path.name[len(pid) + 1:],
        path=path,
        title=meta.get("title", ""),
        type=ptype,
        level=meta.get("level", ""),
        topic=meta.get("topic"),
        languages=languages,
        output=tuple(meta.get("output") or ()),
        tolerance=float(meta.get("tolerance", 1e-6)),
        sorted_by=_sort_keys(meta.get("sorted_by")),
        statement=statement,
    )
    if ptype == "coding":
        problem.tables = _load_tables(path)
        if not problem.output:
            raise ProblemError(f"{pid}: coding problems need 'output'")
    return problem


def _folders() -> list[Path]:
    if not PROBLEMS_DIR.is_dir():
        return []
    return [
        p for p in sorted(PROBLEMS_DIR.iterdir())
        if p.is_dir() and not p.name.startswith((".", "_"))
    ]


def all_problems() -> list[Problem]:
    """Every problem, ordered by title (ids are random, so not by id)."""
    return sorted((load(p) for p in _folders()), key=lambda p: p.title)


def new_id(rng=None) -> str:
    """A random id that no existing problem uses."""
    import secrets

    choice = rng.choice if rng else secrets.choice
    taken = {f.name.split("-", 1)[0] for f in _folders()}
    while True:
        pid = "".join(choice(ID_ALPHABET) for _ in range(ID_LENGTH))
        if pid not in taken:
            return pid


def find(ref: str) -> Problem:
    """Accept an id (`k7q2`), a folder name, or a unique slug prefix."""
    ref = ref.strip().lower().removeprefix("problems/").rstrip("/")
    folders = [f for f in _folders() if "-" in f.name]
    for folder in folders:
        if ref in (folder.name, folder.name.split("-", 1)[0]):
            return load(folder)
    by_slug = [f for f in folders if f.name.split("-", 1)[1].startswith(ref)]
    if len(by_slug) == 1:
        return load(by_slug[0])
    if len(by_slug) > 1:
        names = ", ".join(f.name for f in by_slug)
        raise ProblemError(f"'{ref}' matches several problems: {names}")
    raise ProblemError(
        f"no problem matches '{ref}'; use its id (see `drill status`) or "
        "the start of its name"
    )
