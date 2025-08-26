#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A pre-commit hook to ensure that when a database column is dropped in an
Alembic migration, the corresponding SQLAlchemy model field was marked
as `deferred` in latest master.

This enforces a safe, two-step column removal process:
1. First commit: Mark the column as deferred in the model.
2. Second commit: Remove the column from the model and generate the migration.
"""
import argparse
import ast
import os
import re
import subprocess
import sys
from typing import List, NamedTuple, Optional, Sequence


class DroppedColumn(NamedTuple):
    """Represents a column dropped in a migration."""

    table_name: str
    column_name: str
    migration_file: str
    line_no: int


def get_dropped_column_info_from_op_drop(file_path: str, sub_node: ast.Call) -> DroppedColumn | None:
    """
    Parses an op.drop_column call to extract the table and column names.
    """
    table_name, column_name = None, None

    if len(sub_node.args) > 0 and isinstance(sub_node.args[0], (ast.Constant, ast.Str)):
        table_name = sub_node.args[0].value if isinstance(sub_node.args[0], ast.Constant) else sub_node.args[0].s
    if len(sub_node.args) > 1 and isinstance(sub_node.args[1], (ast.Constant, ast.Str)):
        column_name = sub_node.args[1].value if isinstance(sub_node.args[1], ast.Constant) else sub_node.args[1].s

    for keyword in sub_node.keywords:
        if keyword.arg == "table_name" and isinstance(keyword.value, (ast.Constant, ast.Str)):
            table_name = keyword.value.value if isinstance(keyword.value, ast.Constant) else keyword.value.s
        if keyword.arg == "column_name" and isinstance(keyword.value, (ast.Constant, ast.Str)):
            column_name = keyword.value.value if isinstance(keyword.value, ast.Constant) else keyword.value.s

    if table_name and column_name:
        return DroppedColumn(
            table_name=table_name,
            column_name=column_name,
            migration_file=file_path,
            line_no=sub_node.lineno,
        )
    return None


def get_dropped_columns_info_from_op_execute(file_path: str, sub_node: ast.Call) -> List[DroppedColumn]:
    """
    Parses an op.execute call to extract the table and column names.
    """

    dropped_columns = []

    # Regex to parse 'ALTER TABLE ... DROP COLUMN ...' from op.execute.
    # It handles optional quotes around identifiers.
    # The pattern ensures we match DROP COLUMN or DROP followed by a column name,
    # but not DROP CONSTRAINT or other DROP statements.
    drop_column_sql_re = re.compile(
        r"ALTER\s+TABLE\s+[\"`']?(\w+)[\"`']?\s+DROP\s+"
        r"(?!(?:CONSTRAINT|INDEX|KEY|PRIMARY\s+KEY|FOREIGN\s+KEY)\s+)"
        r"(?:COLUMN\s+)?[\"`']?(\w+)[\"`']?(?:\s|$|;)",
        re.IGNORECASE,
    )

    if not sub_node.args or not isinstance(sub_node.args[0], (ast.Constant, ast.Str)):
        # We can only parse raw string literals. Skip dynamic/variable SQL.
        return []

    sql_statement = sub_node.args[0].value if isinstance(sub_node.args[0], ast.Constant) else sub_node.args[0].s

    for match in drop_column_sql_re.finditer(sql_statement):
        table_name, column_name = match.groups()
        if table_name and column_name:
            dropped_columns.append(
                DroppedColumn(
                    table_name=table_name,
                    column_name=column_name,
                    migration_file=file_path,
                    line_no=sub_node.lineno,
                )
            )

    return dropped_columns


def find_dropped_columns_in_file(file_path: str) -> List[DroppedColumn]:
    """
    Parses a staged Alembic migration file and finds all column drop operations
    within the `upgrade` function. This handles `op.drop_column` and `op.execute`
    with 'ALTER TABLE ... DROP COLUMN ...' statements.

    Args:
        file_path: The path to the Alembic migration .py file.

    Returns:
        A list of DroppedColumn objects found in the file.
    """
    dropped_columns = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            tree = ast.parse(content, filename=file_path)
    except (IOError, SyntaxError) as e:
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        return []

    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and node.name == "upgrade"):
            continue

        for sub_node in ast.walk(node):
            # We are only interested in function calls
            if not isinstance(sub_node, ast.Call):
                continue

            # Check if it's a call to an 'op' method
            if not (
                isinstance(sub_node.func, ast.Attribute)
                and isinstance(sub_node.func.value, ast.Name)
                and sub_node.func.value.id == "op"
            ):
                continue

            # --- Case 1: op.drop_column() ---
            if sub_node.func.attr == "drop_column":
                dropped_column = get_dropped_column_info_from_op_drop(file_path=file_path, sub_node=sub_node)

                if dropped_column:
                    dropped_columns.append(dropped_column)

            # --- Case 2: op.execute('...') ---
            elif sub_node.func.attr == "execute":
                for dropped_col in get_dropped_columns_info_from_op_execute(file_path=file_path, sub_node=sub_node):
                    dropped_columns.append(dropped_col)

    return dropped_columns


def find_model_file(table_name: str, search_path: str) -> Optional[str]:
    """
    Scans a directory for a Python file that defines a SQLAlchemy model
    with a `__tablename__` matching the given table name. This searches the
    current filesystem to find the file's location.
    """
    tablename_pattern = re.compile(rf"__tablename__\s*=\s*['\"]{re.escape(table_name)}['\"]")

    for root, _, files in os.walk(search_path):
        if any(d in root for d in ("/site-packages/", "/.venv/", "/alembic/")):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                try:
                    # We check the staged/working file to find the path, because
                    # the file might be new or renamed in this commit.
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        if tablename_pattern.search(content):
                            # We found a file that at least *mentions* the tablename.
                            # We will later confirm with git that it's the right one.
                            return os.path.normpath(file_path)
                except (IOError, UnicodeDecodeError):
                    continue
    return None


def get_file_content_from_master(file_path: str) -> Optional[str]:
    """
    Retrieves the content of a file from the latest master commit using git.
    Returns None if the file did not exist in latest master or an error occurs.
    """
    if not file_path:
        return None
    try:
        command = ["git", "show", f"origin/master:{file_path.replace(os.sep, '/')}"]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,  # Don't throw exception on non-zero exit
        )
        if result.returncode == 0:
            return result.stdout
        # Any non-zero return code means the file likely didn't exist in latest
        # master or another git error occurred.
        return None
    except (FileNotFoundError, subprocess.CalledProcessError):
        # Git not found or other process error.
        return None


def check_column_in_content(content: str, table_name: str, column_name: str) -> bool:
    """
    Parses string content with AST to determine if a specific column
    attribute is present and wrapped in `deferred()`.
    """
    try:
        tree = ast.parse(content)
    except (IOError, SyntaxError):
        return False

    target_class_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for class_body_node in node.body:
                if (
                    isinstance(class_body_node, ast.Assign)
                    and len(class_body_node.targets) == 1
                    and isinstance(class_body_node.targets[0], ast.Name)
                    and class_body_node.targets[0].id == "__tablename__"
                    and isinstance(class_body_node.value, (ast.Constant, ast.Str))
                    and (
                        class_body_node.value.value
                        if isinstance(class_body_node.value, ast.Constant)
                        else class_body_node.value.s
                    )
                    == table_name
                ):
                    target_class_node = node
                    break
        if target_class_node:
            break

    if not target_class_node:
        return False

    for node in target_class_node.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == column_name
        ):
            # The column exists. Now check if it's deferred.
            if (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id == "deferred"
            ):
                return True  # It is deferred.
            return False  # It exists, but is NOT deferred.

    return False  # The column attribute was not found in the class.


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help="Filenames to check.")
    parser.add_argument(
        "--models-path",
        dest="models_path",
        default=".",
        help="The root path to search for model files.",
    )
    parser.add_argument(
        "--db-migrations-path",
        dest="migrations_path",
        default=".",
        help="The root path to search for DB migrations files.",
    )
    args = parser.parse_args(argv)

    has_errors = False

    migration_files = [f for f in args.filenames if args.migrations_path in f.replace("\\", "/") and f.endswith(".py")]

    if not migration_files:
        return 0

    print("Running Alembic deferred column check...")
    for filename in migration_files:
        dropped_columns = find_dropped_columns_in_file(filename)
        if not dropped_columns:
            continue

        for col in dropped_columns:
            print(f"  - Found drop_column for '{col.table_name}.{col.column_name}' in {col.migration_file}")

            # Note: This assumes the model file was not renamed in the same commit.
            model_path = find_model_file(col.table_name, args.models_path)

            if not model_path:
                print(
                    f"    WARNING: Could not find a model file for table '{col.table_name}'. "
                    f"Please check manually.",
                    file=sys.stderr,
                )
                continue

            latest_master_content = get_file_content_from_master(model_path)

            if not latest_master_content:
                print(
                    f"    ERROR: Model file '{model_path}' seems to be new. "
                    f"You cannot drop a column from a newly created model.",
                    file=sys.stderr,
                )
                has_errors = True
                continue

            if not check_column_in_content(latest_master_content, col.table_name, col.column_name):
                print("-" * 70, file=sys.stderr)
                print(
                    f"ERROR: Column '{col.column_name}' is dropped from table '{col.table_name}'\n"
                    f"       in migration: {col.migration_file} (line {col.line_no})\n"
                    f"       but it was NOT marked as `deferred` in latest master for model '{model_path}'.",
                    file=sys.stderr,
                )
                print(
                    "\nPlease follow a two-step process:\n"
                    "       1. Submit a PR and deploy a change where the column is marked `deferred()`.\n"
                    "       2. ONCE THE FIRST PR IS DEPLOYED, in a second PR, remove the column and generate"
                    "this migration.",
                    file=sys.stderr,
                )
                print("-" * 70, file=sys.stderr)
                has_errors = True

    if has_errors:
        print("\nCheck failed. Please fix your models/migrations and try committing again.")
        return 1

    print("Check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
