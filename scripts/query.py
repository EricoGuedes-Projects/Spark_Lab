import duckdb

con = duckdb.connect()

result = con.execute("""
SELECT
    category,
    COUNT(*) total
FROM 'data/curated/**/*.parquet'
GROUP BY category
ORDER BY total DESC
""").fetchdf()

print(result)