# Load Snowflake to Polars

Load data from snowflake to Polars DataFrame directly.

## Usage

```python

query = "Select * from <TABLE_NAME> limit 10"
sp = SnowflakePl()
df = sp.run_sql(query)
print(df)

```

## .env File

A `.env` should be created in the project directory with the follow keys:

```
USERNAME="USERNAME"
ACCOUNT="ACCOUNT"
PASSWORD="PASSWORD"
```

`ACCOUNT` is from the URL `<ACCOUNT>.snowflakecomputing.com`
