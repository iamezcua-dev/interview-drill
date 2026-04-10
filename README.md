# Interview Practice

## Objective

This is an effort to help you warm up for your upcoming interviews.

You're expected to work on a separate branch created from `main`, which can be used to get the latest problems as I add more.

## Problem solving progress

Here you can see how many problems you have solved so far.

```
░░░░░░░░░░░░░░░░░░░░  0 / 15 done (0%)
```

See problem list [here](#problem-list).

## How to know you have properly solved a particular code problem?

By running its associated test harness. See the instructions below.

> [!NOTE]
>
> - All commands must be run from the project root.
> - Replace \<**N**\> with the actual question name.

### 📊 SQL problems

- Run both DDL and tests cases for a specific question:

  ```bash
  export QUESTION_NUMBER='q<N>' \
      && sqlcmd \
          -S localhost,1433 \
          -N disable \
          -i sql/{ddl/${QUESTION_NUMBER}_ddl,tests/${QUESTION_NUMBER}_tests}.sql
  ```

> [!IMPORTANT]
> Make sure you have properly set `SQLCMDUSER` and `SQLCMDPASSWORD` in your system. More info [here](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility?view=sql-server-ver17&tabs=go%2Cwindows-support&pivots=cs1-bash#-p-password).

### ⚡️ PySpark problems

- Run tests cases for a specific question:

  ```bash
  export QUESTION_NUMBER='q<N>' \
      && poetry run pytest src/test/python/test_${QUESTION_NUMBER}.py -v
  ```

- Run a solution code directly (while attempting your solution or debugging):

  ```bash
  export QUESTION_NUMBER='q<N>' \
      && poetry run python src/main/python/${QUESTION_NUMBER}_runner.py
  ```

### 📖 Theoretical problems

- Ask AI or find the answer yourself. Once you're ok with the answer, please add the word **SOLVED** at the very last line.

---

## Problem list

| Status | #       | Type    | Overview                                                  |
| :----: | ------- | ------- | --------------------------------------------------------- |
|   🔘 | **Q1**  | SQL     | Highest-sales employee(s) per department (with ties)      |
|   🔘 | **Q2**  | SQL     | Running total of sales per department, ordered by date    |
|   🔘 | **Q3**  | Theory  | Transformation vs action in Apache Spark                  |
|   🔘 | **Q4**  | PySpark | Employees earning strictly above their department average |
|   🔘 | **Q5**  | SQL     | Customers with orders in every month of 2024              |
|   🔘 | **Q6**  | Theory  | WHERE vs HAVING in SQL                                    |
|   🔘 | **Q7**  | PySpark | Month-over-month revenue change                           |
|   🔘 | **Q8**  | PySpark | Total spent per user, ordered descending                  |
|   🔘 | **Q9**  | Theory  | INNER JOIN vs LEFT JOIN vs CROSS JOIN                     |
|   🔘 | **Q10** | PySpark | Avg salary per department filtered > 70,000, ordered desc |
|   🔘 | **Q11** | Theory  | Lazy evaluation in Spark                                  |
|   🔘 | **Q12** | SQL     | Top 3 customers by total amount spent                     |
|   🔘 | **Q13** | PySpark | Most recently visited page per user                       |
|   🔘 | **Q14** | Theory  | CTE vs subquery in SQL                                    |
|   🔘 | **Q15** | SQL     | Department employee count (including zero-employee depts) |

---

## Questions?

Send an email to (iamezcua.dev@gmail.com), but don't expect a quick answer.
