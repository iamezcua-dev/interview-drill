from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date
from q13_solution import solution

TITLE = "Q13 [PySpark | Coding | Senior] — For each user, return the most recently visited page. If a user visited multiple pages on the same latest date, return all of them."


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("q13_dev")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("user_id",    IntegerType(), False),
        StructField("page",       StringType(),  False),
        StructField("visit_date", DateType(),    False),
    ])

    data = [
        (1, "home",    date(2024, 1, 5)),
        (1, "about",   date(2024, 1, 3)),
        (1, "contact", date(2024, 1, 5)),
        (2, "home",    date(2024, 1, 7)),
        (2, "pricing", date(2024, 1, 2)),
        (3, "blog",    date(2024, 1, 1)),
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
