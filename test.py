from sqlglot import parse_one, exp
from sqlglot.executor import execute
from sqlglot.optimizer import optimize
from tests.helpers import TPCDS_SCHEMA
import os
import duckdb

sql = """
SELECT yi
FROM   (SELECT ib_lower_bound yi FROM income_band, reason) y,
       (SELECT ib_lower_bound zi FROM income_band) z;
"""

DIR_TPCDS = "./tests/fixtures/optimizer/tpc-ds/"


conn = duckdb.connect()

for table, columns in TPCDS_SCHEMA.items():
    conn.execute(
        f"""
        CREATE VIEW {table} AS
        SELECT *
        FROM READ_CSV('{DIR_TPCDS}{table}.csv.gz', delim='|', header=True, columns={columns})
        """
    )

def to_csv(expression):
    if isinstance(expression, exp.Table) and os.path.exists(
        f"{DIR_TPCDS}{expression.name}.csv.gz"
    ):
        return parse_one(
            f"READ_CSV('{DIR_TPCDS}{expression.name}.csv.gz', 'delimiter', '|') AS {expression.alias_or_name}"
        )
    return expression

print()
table = execute(parse_one(sql).transform(to_csv).sql(pretty=True), TPCDS_SCHEMA)
print(table)


