# Interview SQL Drill

## Running Test Cases

All commands must be run from the project root.

Note: replace \<**N**\> with the actual question name.

### SQL (MSSQL)

Run DDL and tests for a specific question:

```bash
export QUESTION_NUMBER='q<N>' && sqlcmd -S localhost,1433 -U sa -P 'Eddiejob$' -N disable -i sql/{ddl/${QUESTION_NUMBER}_ddl,tests/${QUESTION_NUMBER}_tests}.sql
```

### PySpark

Run tests for a specific question:

```bash
export QUESTION_NUMBER='q<N>' && poetry run pytest src/test/python/test_${QUESTION_NUMBER}.py -v
```

Run a solution directly (for development/debugging):

```bash
export QUESTION_NUMBER='q<N>' && poetry run python src/main/python/${QUESTION_NUMBER}_runner.py
```
