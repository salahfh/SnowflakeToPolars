# Load Snowflake to Polars

Load data from snowflake to Polars DataFrame directly.

## Usage

```python

query = "Select * from <TABLE_NAME> limit 10"
sp = SnowflakePl()
df = sp.run_sql(query)
print(df)

```
