"""Tests of tools/check-matrix.py: it passes on a consistent tree and fails, naming the
problem, when a test reference, a cell or the grid is wrong.

Run: python3 -m unittest discover -s tools -p 'test_*.py'
"""
import importlib.util
import tempfile
import textwrap
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("check_matrix", Path(__file__).with_name("check-matrix.py"))
check_matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_matrix)

LANGUAGES = ["Go", "C++", "Rust", "Python", "Node", "TS"]
TESTS = {
    "Go": "`go/pkg::TestA`",
    "C++": "`ctest::xrpc_cpp_foo`",
    "Rust": "`rust/tests/t.rs::r_test`",
    "Python": "`python/tests/test_x.py::Case::test_m`",
    "Node": "`node/test/n.test.cjs::node title`",
    "TS": "`ts/test/t.test.mjs::ts title`",
}


def matrix(edit=None):
    """A matrix document with one row; edit may change a cell or the grid."""
    rows = {lang: [lang, "C, S", "keep", "a real consumer", TESTS[lang]] for lang in LANGUAGES}
    rows["TS"] = ["TS", "C, S", "n/a", "browsers cannot do this `absent:ts/gone.ts`", "none"]
    rows["Rust"] = ["Rust", "C", "keep", "a real consumer", TESTS["Rust"]]
    grid = ["row one", "keep", "keep", "keep", "keep", "keep", "n/a"]
    extra = [["Rust", "S", "new", "another consumer", TESTS["Rust"]]]
    grid[3] = "C keep, S new"
    for lang in ("Go", "C++", "Python", "Node"):
        rows[lang][1] = "C"
    # Go, C++, Python and Node: a client cell only.
    for i, lang in enumerate(LANGUAGES):
        if lang in ("Go", "C++", "Python", "Node"):
            grid[i + 1] = "C keep"
    state = {"rows": rows, "extra": extra, "grid": grid}
    if edit:
        edit(state)
    lines = ["# matrix", "", "## Matrix", "",
             "| Profile / mode | " + " | ".join(LANGUAGES) + " |", "| --- |" + " --- |" * 6,
             "| " + " | ".join(state["grid"]) + " |", "", "## Cells", "", "### row one", "",
             "| Language | Role | Status | Consumers / reason | Tests |", "| --- | --- | --- | --- | --- |"]
    ordered = sorted(list(state["rows"].values()) + state["extra"], key=lambda r: LANGUAGES.index(r[0]))
    lines += ["| " + " | ".join(r) + " |" for r in ordered]
    return "\n".join(lines) + "\n"


def build(root, matrix_text, extra_files=None):
    files = {
        "docs/capability-matrix.md": matrix_text,
        "cpp/CMakeLists.txt": "xgc2_xrpc_test(foo tests/foo.cpp)\n",
        "go/pkg/a_test.go": "package pkg\nfunc TestA(t *testing.T) {}\n",
        "python/tests/test_x.py": "class Case:\n    def test_m(self):\n        pass\n",
        "rust/tests/t.rs": "#[test]\nfn r_test() {}\n",
        "node/test/n.test.cjs": 'test("node title", () => {});\n',
        "ts/test/t.test.mjs": "test('ts title', async () => {});\n",
    }
    for name in check_matrix.PROOF_FILES[1:]:
        files[name] = "no references\n"
    files.update(extra_files or {})
    for name, content in files.items():
        path = Path(root) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)


class CheckMatrix(unittest.TestCase):
    def problems(self, edit=None, extra_files=None):
        with tempfile.TemporaryDirectory() as root:
            build(root, matrix(edit), extra_files)
            problems, _ = check_matrix.check(Path(root))
            return problems

    def assertFails(self, problems, text):
        self.assertTrue(any(text in p for p in problems), "expected a problem mentioning %r, got %r" % (text, problems))

    def test_a_consistent_tree_passes(self):
        self.assertEqual(self.problems(), [])

    def test_a_keep_cell_without_a_test_fails(self):
        self.assertFails(self.problems(lambda s: s["rows"]["Go"].__setitem__(4, "none")), "names at least one test")

    def test_a_keep_cell_without_a_consumer_fails(self):
        self.assertFails(self.problems(lambda s: s["rows"]["Go"].__setitem__(3, "")), "names its consumers")

    def test_an_na_cell_without_a_reason_fails(self):
        self.assertFails(self.problems(lambda s: s["rows"]["TS"].__setitem__(3, "-")), "gives its reason")

    def test_a_reference_to_a_missing_test_fails(self):
        for reference in ("`go/pkg::TestMissing`", "`ctest::xrpc_cpp_missing`", "`python/tests/test_x.py::Case::test_missing`",
                          "`rust/tests/t.rs::missing`", "`node/test/n.test.cjs::missing title`", "`ts/test/t.test.mjs::missing title`",
                          "`go/nowhere::TestA`", "`python/tests/gone.py::Case::test_m`"):
            with self.subTest(reference):
                self.assertFails(self.problems(lambda s: s["rows"]["Go"].__setitem__(4, reference)), "is not a test in the tree")

    def test_a_function_that_is_not_a_test_does_not_count(self):
        files = {"rust/tests/t.rs": "fn r_test() {}\n", "python/tests/test_x.py": "class Case:\n    def helper(self):\n        pass\n"}
        problems = self.problems(extra_files=files)
        self.assertFails(problems, "`rust/tests/t.rs::r_test` is not a test")
        self.assertFails(problems, "`python/tests/test_x.py::Case::test_m` is not a test")

    def test_a_grid_that_disagrees_with_the_cells_fails(self):
        self.assertFails(self.problems(lambda s: s["grid"].__setitem__(1, "C new")), "grid row 'row one'")

    def test_a_missing_language_fails(self):
        self.assertFails(self.problems(lambda s: s["rows"].pop("Node")), "no entry for Node")

    def test_a_malformed_row_fails(self):
        self.assertFails(self.problems(lambda s: s["rows"]["Go"].__setitem__(2, "maybe")), "unknown status")
        self.assertFails(self.problems(lambda s: s["rows"]["Go"].__setitem__(1, "X")), "role must be")

    def test_a_removed_cell_whose_code_is_back_fails(self):
        self.assertFails(self.problems(extra_files={"ts/gone.ts": "export {};\n"}), "still exists")

    def test_references_in_the_contracts_are_checked_too(self):
        files = {"contracts/runtime.md": "see `go/pkg::TestNope`\n"}
        self.assertFails(self.problems(extra_files=files), "contracts/runtime.md")

    def test_the_grid_the_cells_imply_can_be_printed(self):
        with tempfile.TemporaryDirectory() as root:
            build(root, matrix())
            problems, result = check_matrix.check(Path(root))
            self.assertEqual(problems, [])
            grid = check_matrix.render(result[2])
            self.assertIn("| row one | C keep | C keep | C keep, S new | C keep | C keep | n/a |", grid)


if __name__ == "__main__":
    unittest.main()
