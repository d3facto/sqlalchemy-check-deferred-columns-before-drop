# sqlalchemy-check-deferred-columns-before-drop

Git pre-commit hook to check columns in SQLAlchemy models are marked deferred before dropping them in a DB migration.

## Usage

Usage with `.pre-commit-config.yaml`:
------------------------------------
repos:
- repo: local
    hooks:
        -   id: sqlalchemy-check-deferred-columns-before-drop
            name: DB migration - Check for deferred columns on drop
            # Important: Pass staged files to the script
            pass_filenames: true
            files: ^db/public/versions/.*\.py$
            args: ['--models-path', 'db/public', '--db-migrations-path', 'db/public/versions']