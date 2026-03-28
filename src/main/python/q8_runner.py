from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from q8_solution import solution

TITLE = "Q8 [PySpark | Coding | Senior] — Given a DataFrame of transactions, return user_id and total_spent (sum of quantity * price per user), ordered by total_spent descending."


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("q8_dev")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("user_id",    IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity",   IntegerType(), False),
        StructField("price",      DoubleType(),  False),
    ])

    data = [
        (1, 101, 2, 9.99),
        (2, 102, 1, 49.99),
        (1, 103, 3, 5.00),
        (3, 101, 1, 9.99),
        (2, 104, 2, 15.00),
        (3, 105, 4, 3.50),
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
