#!/usr/bin/env python3
"""Check docs/capability-matrix.md against the test inventory of the source tree.

The cell tables of the matrix are the source of truth. The check fails when

  * a cell marked keep or new names no consumer or no test, or an n/a or remove cell
    gives no reason;
  * a test reference does not resolve to a test in the tree (the references in
    docs/capability-matrix.md and in contracts/*.md are all checked);
  * the grid at the top of the matrix disagrees with the cell tables
    (`--grid` prints the grid the cell tables imply);
  * a removed cell still has the code it claims to have removed (`absent:` and
    `lacks:` markers).

A test reference is a backtick span with one of these forms:

  go/<directory>::<TestName>              func Test*, Fuzz* in <directory>/*_test.go (go::<TestName> for the root package)
  ctest::<name>                           a test of cpp/CMakeLists.txt
  python/<file>::<Class>::<test_method>   or python/<file>::<test_function>
  rust/<file>::<function>                 a #[test] or #[tokio::test] function
  node/<file>::<title> and ts/<file>::<title>   the title of a test(...) or it(...) call

Python 3.8 or newer, standard library only.
"""
import argparse
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATRIX = "docs/capability-matrix.md"
LANGUAGES = ["Go", "C++", "Rust", "Python", "Node", "TS"]
STATUSES = ["keep", "new", "remove", "n/a"]
ROLES = ["C", "S"]
PROOF_FILES = [MATRIX] + ["contracts/" + name for name in (
    "runtime.md", "udp-v1.md", "events.md", "sessions.md", "operations.md", "fault-conformance.md")]
SPAN = re.compile(r"`([^`\n]+)`")
REFERENCE = re.compile(r"^(?:go(?:/[^:\s]+)?::\w+|ctest::\w+|python/[^:\s]+\.py::\w+(?:::\w+)?|rust/[^:\s]+\.rs::\w+|(?:node|ts)/[^:\s]+\.(?:cjs|mjs|js)::.+)$")
KNOWN = re.compile(r"^(?:go(?:/|::)|ctest::|python/|rust/|node/|ts/)")


# ---- test inventory -------------------------------------------------------------

def read(root, relative):
    path = root / relative
    return path.read_text(encoding="utf-8") if path.is_file() else None


def cmake_tests(root):
    """The ctest names of cpp/CMakeLists.txt, with foreach loops expanded."""
    text = read(root, "cpp/CMakeLists.txt")
    if text is None:
        return set()
    names = set(re.findall(r"add_test\(\s*NAME\s+(\w+)", text))
    for match in re.finditer(r"foreach\(\s*(\w+)\s+([^)]*)\)(.*?)endforeach\(\)", text, re.S):
        variable, values, body = match.group(1), match.group(2).split(), match.group(3)
        for call in re.finditer(r"xgc2_xrpc_test\(\s*\$\{" + variable + r"\}", body):
            names.update("xrpc_cpp_" + value for value in values)
    for call in re.finditer(r"xgc2_xrpc_test\(\s*([A-Za-z]\w*)\s", text):
        names.add("xrpc_cpp_" + call.group(1))
    return names


def go_test_exists(root, directory, name):
    folder = root / directory
    if not folder.is_dir():
        return False
    pattern = re.compile(r"^func " + re.escape(name) + r"\(", re.M)
    return any(pattern.search(f.read_text(encoding="utf-8")) for f in folder.glob("*_test.go"))


def python_test_exists(root, relative, class_name, function):
    source = read(root, relative)
    if source is None:
        return False
    tree = ast.parse(source)
    scopes = [tree] if class_name is None else [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef) and n.name == class_name]
    for scope in scopes:
        for node in scope.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function and node.name.startswith("test"):
                return True
    return False


def rust_test_exists(root, relative, function):
    source = read(root, relative)
    if source is None:
        return False
    for match in re.finditer(r"\bfn\s+" + re.escape(function) + r"\s*[(<]", source):
        before = source[:match.start()].rstrip().splitlines()
        # Walk back over attribute lines and the async keyword.
        for line in reversed(before):
            line = line.strip()
            if line.startswith("#["):
                if re.match(r"#\[(?:tokio::)?test\b", line):
                    return True
                continue
            if line in ("async", "pub", "pub(crate)"):
                continue
            break
    return False


def js_titles(source):
    """The first string literal of every test(...), it(...) and describe(...) call."""
    titles = set()
    for match in re.finditer(r"\b(?:test|it|describe)(?:\.\w+)?\s*\(\s*(['\"`])", source):
        quote, index, out = match.group(1), match.end(), []
        while index < len(source) and source[index] != quote:
            char = source[index]
            if char == "\\" and index + 1 < len(source):
                out.append(source[index + 1])
                index += 2
                continue
            if quote == "`" and char == "$" and source[index + 1:index + 2] == "{":
                out = None  # an interpolated title cannot be referenced
                break
            out.append(char)
            index += 1
        if out is not None:
            titles.add("".join(out))
    return titles


def resolve(root, reference, ctests):
    """True when the reference names a test in the tree."""
    if reference.startswith("ctest::"):
        return reference[len("ctest::"):] in ctests
    if reference.startswith("go"):
        directory, name = reference.split("::", 1)
        return go_test_exists(root, directory, name)
    if reference.startswith("python/"):
        parts = reference.split("::")
        return python_test_exists(root, parts[0], parts[1] if len(parts) == 3 else None, parts[-1])
    if reference.startswith("rust/"):
        relative, function = reference.split("::", 1)
        return rust_test_exists(root, relative, function)
    relative, title = reference.split("::", 1)
    source = read(root, relative)
    return source is not None and title in js_titles(source)


# ---- the matrix document --------------------------------------------------------

def table_rows(lines):
    """The cells of the markdown table that starts at lines[0], without separator rows."""
    rows = []
    for line in lines:
        if not line.startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if all(re.fullmatch(r":?-{3,}:?", c) for c in cells):
            continue
        rows.append(cells)
    return rows


def parse_matrix(text, problems):
    lines = text.splitlines()
    grid, cells = None, {}
    top = None
    for number, line in enumerate(lines):
        if not (line.startswith("## ") or line.startswith("### ")):
            continue
        title = line.lstrip("#").strip()
        if line.startswith("## "):
            top = title
        start = number + 1
        while start < len(lines) and not lines[start].startswith("|") and not lines[start].startswith("#"):
            start += 1
        if start >= len(lines) or not lines[start].startswith("|"):
            continue
        rows = table_rows(lines[start:])
        if line.startswith("## ") and title == "Matrix":
            grid = rows
        elif line.startswith("### ") and top == "Cells":
            cells[title] = rows
    if grid is None:
        problems.append("no '## Matrix' grid in " + MATRIX)
    return grid, cells


def cell_entries(name, rows, problems):
    """Entries of one cell table: (language, role, status, text, tests), validated."""
    header = rows[0] if rows else []
    if [c.lower() for c in header] != ["language", "role", "status", "consumers / reason", "tests"]:
        problems.append("%s: the table needs the columns Language | Role | Status | Consumers / reason | Tests" % name)
        return []
    entries, seen = [], set()
    for row in rows[1:]:
        if len(row) != 5:
            problems.append("%s: a row does not have 5 columns: %s" % (name, row))
            continue
        language, role, status, text, tests = row
        label = "%s / %s %s" % (name, language, role)
        roles = [r.strip() for r in role.split(",")]
        if language not in LANGUAGES:
            problems.append("%s: unknown language %r" % (label, language))
        if status not in STATUSES:
            problems.append("%s: unknown status %r" % (label, status))
        if not roles or any(r not in ROLES for r in roles) or len(set(roles)) != len(roles):
            problems.append("%s: role must be C, S or 'C, S'" % label)
        if status in ("keep", "new") and len(roles) != 1:
            problems.append("%s: a keep or new row has exactly one role" % label)
        for r in roles:
            key = (language, r)
            if key in seen:
                problems.append("%s: listed twice" % label)
            seen.add(key)
        refs = SPAN.findall(tests)
        refs = [r for r in refs if "::" in r]
        if status in ("keep", "new"):
            if not text.strip() or text.strip() in ("-", "—"):
                problems.append("%s: a %s cell names its consumers" % (label, status))
            if not refs:
                problems.append("%s: a %s cell names at least one test" % (label, status))
        elif not text.strip() or text.strip() in ("-", "—"):
            problems.append("%s: an %s cell gives its reason" % (label, status))
        entries.append((language, roles, status, text, refs))
    return entries


def expected_grid_cell(entries, language):
    own = [e for e in entries if e[0] == language]
    if not own:
        return None
    parts = []
    for _, roles, status, _, _ in sorted(own, key=lambda e: ROLES.index(e[1][0]) if e[1][0] in ROLES else len(ROLES)):
        parts.append(status if len(roles) == 2 else "%s %s" % (roles[0], status))
    return ", ".join(parts)


def expected_grid(cells, problems):
    table = [["Profile / mode"] + LANGUAGES]
    model = {}
    for name, rows in cells.items():
        entries = cell_entries(name, rows, problems)
        model[name] = entries
        row = [name]
        for language in LANGUAGES:
            text = expected_grid_cell(entries, language)
            if text is None:
                problems.append("%s: no entry for %s" % (name, language))
                text = "?"
            row.append(text)
        table.append(row)
    return table, model


def render(table):
    return "\n".join("| " + " | ".join(row) + " |" for row in
                     [table[0], ["---"] * len(table[0])] + table[1:])


# ---- main -----------------------------------------------------------------------

def check(root):
    problems = []
    text = read(root, MATRIX)
    if text is None:
        return ["missing " + MATRIX], None
    grid, cells = parse_matrix(text, problems)
    if not cells:
        problems.append("no cell tables ('### <profile / mode>' headings) in " + MATRIX)
    expected, model = expected_grid(cells, problems)
    if grid is not None:
        names = [row[0] for row in grid[1:]]
        if names != list(cells):
            problems.append("the grid rows %s differ from the cell tables %s" % (names, list(cells)))
        if grid[0] != expected[0]:
            problems.append("the grid header is %s, expected %s" % (grid[0], expected[0]))
        for row, want in zip(grid[1:], expected[1:]):
            if row != want:
                problems.append("grid row %r is %s but the cell tables say %s" % (row[0], row[1:], want[1:]))
    # Every reference in the matrix and in the contracts must resolve.
    ctests = cmake_tests(root)
    references = {}
    for relative in PROOF_FILES:
        source = read(root, relative)
        if source is None:
            problems.append("missing " + relative)
            continue
        for span in SPAN.findall(source):
            if "::" in span and KNOWN.match(span) and "<" not in span:
                references.setdefault(span, relative)
    for reference, relative in sorted(references.items()):
        if not REFERENCE.match(reference):
            problems.append("%s: malformed test reference `%s`" % (relative, reference))
        elif not resolve(root, reference, ctests):
            problems.append("%s: `%s` is not a test in the tree" % (relative, reference))
    # Claims of absence: `absent:<path>` and `lacks:<path>:<text>`.
    for name, entries in model.items():
        for language, roles, status, reason, _ in entries:
            if status not in ("remove", "n/a"):
                continue
            for span in SPAN.findall(reason):
                if span.startswith("absent:") and (root / span[7:]).exists():
                    problems.append("%s / %s: `%s` still exists" % (name, language, span[7:]))
                if span.startswith("lacks:"):
                    path, _, needle = span[6:].partition(":")
                    content = read(root, path)
                    if content is None:
                        problems.append("%s / %s: %s does not exist" % (name, language, path))
                    elif needle in content:
                        problems.append("%s / %s: %s still contains %r" % (name, language, path, needle))
    counts = {status: sum(1 for entries in model.values() for e in entries if e[2] == status) for status in STATUSES}
    return problems, (counts, len(references), expected)


def main():
    parser = argparse.ArgumentParser(description="Check the capability matrix against the test inventory.")
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root (default: this checkout)")
    parser.add_argument("--grid", action="store_true", help="print the grid the cell tables imply and exit")
    args = parser.parse_args()
    problems, result = check(args.root.resolve())
    if args.grid:
        if result is not None:
            print(render(result[2]))
        sys.exit(1 if problems and result is None else 0)
    if problems:
        sys.stderr.write("capability matrix check failed:\n")
        for problem in problems:
            sys.stderr.write("  " + problem + "\n")
        sys.exit(1)
    counts, references, _ = result
    print("capability matrix: %s; %d test references resolve" % (
        ", ".join("%d %s" % (counts[s], s) for s in STATUSES), references))


if __name__ == "__main__":
    main()
