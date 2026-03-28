from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from q10_solution import solution

TITLE = "Q10 [PySpark | Coding | Senior] — Given a DataFrame of employees, return department and avg_salary per department, only for departments where avg_salary > 70000, ordered by avg_salary descending."


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("q10_dev")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("department",  StringType(),  False),
        StructField("salary",      DoubleType(),  False),
        StructField("hire_date",   DateType(),    False),
    ])

    data = [
        (1, "Engineering", 95000.0, date(2020, 1, 15)),
        (2, "Engineering", 85000.0, date(2019, 6, 1)),
        (3, "Marketing",   60000.0, date(2021, 3, 10)),
        (4, "Marketing",   65000.0, date(2022, 7, 20)),
        (5, "Sales",       72000.0, date(2020, 11, 5)),
        (6, "Sales",       68000.0, date(2021, 9, 14)),
    ]

    df = spark.createDataFrame(data, schema)

    import textwrap
    top    = "╔" + "═" * 58 + "╗"
    bottom = "╚" + "═" * 58 + "╝"
    print(top)
    for line in textwrap.wrap(TITLE, width=56):
        print(f"║ {line:<56} ║")
    print(bottom)
    print("=== Input ===")
    df.show()

    print("=== Result ===")
    result = solution(df)
    if result is not None:
        result.show()
    else:
        print("solution() not implemented yet.")

    spark.stop()


if __name__ == "__main__":
    main()
