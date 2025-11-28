"""
Syntax validation tests for Notion connector source files.
"""

import ast
import py_compile
import sys
from pathlib import Path

import pytest


SRC_DIR = Path(__file__).parent.parent / "src"


class TestSyntaxValidation:
    """Test that all Python source files have valid syntax."""

    SOURCE_FILES = [
        "connector.py",
        "config.py",
        "auth.py",
        "client.py",
        "streams.py",
        "utils.py",
        "__init__.py",
    ]

    @pytest.mark.parametrize("filename", SOURCE_FILES)
    def test_source_file_syntax(self, filename: str):
        """Test that source file has valid Python syntax."""
        filepath = SRC_DIR / filename
        assert filepath.exists(), f"Source file {filename} does not exist"

        # Try to compile the file
        try:
            py_compile.compile(str(filepath), doraise=True)
        except py_compile.PyCompileError as e:
            pytest.fail(f"Syntax error in {filename}: {e}")

    @pytest.mark.parametrize("filename", SOURCE_FILES)
    def test_source_file_parseable(self, filename: str):
        """Test that source file is parseable as AST."""
        filepath = SRC_DIR / filename
        assert filepath.exists(), f"Source file {filename} does not exist"

        with open(filepath, "r") as f:
            source = f.read()

        try:
            ast.parse(source, filename=filename)
        except SyntaxError as e:
            pytest.fail(f"AST parse error in {filename}: {e}")

    def test_all_source_files_exist(self):
        """Test that all expected source files exist."""
        missing = []
        for filename in self.SOURCE_FILES:
            filepath = SRC_DIR / filename
            if not filepath.exists():
                missing.append(filename)

        assert not missing, f"Missing source files: {missing}"

    def test_source_directory_exists(self):
        """Test that the src directory exists."""
        assert SRC_DIR.exists(), f"Source directory {SRC_DIR} does not exist"
        assert SRC_DIR.is_dir(), f"{SRC_DIR} is not a directory"
