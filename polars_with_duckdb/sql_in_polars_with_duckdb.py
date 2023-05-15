import polars as pl
import duckdb

# read in data
df_pl = pl.scan_csv('../sample_data.csv')

# filtering in duckdb
df_duckdb_filtered = duckdb.query("""
  select 
    *
  from df_pl
  where Name = 'Mike'
  
""")
print(df_duckdb_filtered)

# a window function in duckdb
df_duckdb_rank = duckdb.query("""
  select 
    *,
    rank() over(order by FirstEnrolledDate) as EnrolledRank

  from df_pl
  
""")
print(df_duckdb_rank)

# convert duckdb to polars dataframe
duckdb_to_polars = df_duckdb_rank.pl()
print(duckdb_to_polars)
