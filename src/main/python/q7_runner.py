from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from q7_solution import solution

TITLE = "Q7 [PySpark | Coding | Senior] — Given a DataFrame of monthly revenue, compute the month-over-month revenue change."


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("q7_dev")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("month",   IntegerType(), False),
        StructField("revenue", IntegerType(), False),
    ])

    data = [
        (1, 10000),
        (2, 12000),
        (3,  9500),
        (4, 11000),
        (5, 13500),
        (6, 12800),
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
