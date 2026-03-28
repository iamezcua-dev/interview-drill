from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from q4_solution import solution

TITLE = "Q4 [PySpark | Coding | Senior] — Return employees whose salary is strictly above their department average."


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("q4_dev")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("name",        StringType(),  False),
        StructField("department",  StringType(),  False),
        StructField("salary",      IntegerType(), False),
    ])

    data = [
        (1, "Alice", "APAC", 80000),
        (2, "Bob",   "APAC", 60000),
        (3, "Carol", "APAC", 90000),
        (4, "Dave",  "EMEA", 70000),
        (5, "Eve",   "EMEA", 75000),
        (6, "Frank", "EMEA", 65000),
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
