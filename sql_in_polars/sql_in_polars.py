import polars as pl

# read in data
df_pl = pl.scan_csv('../sample_data.csv').collect()

# prep for sql execution
sql = pl.SQLContext()
sql.register('df_pl', df_pl)

result_df = sql.execute(
    """
      select 
        *
      from df_pl
      where Name = 'Mike'
    """
).collect()

print(result_df)

"""
output:
shape: (1, 4)
┌───────────┬──────┬─────┬───────────────────┐
│ studentId ┆ Name ┆ Age ┆ FirstEnrolledDate │
│ ---       ┆ ---  ┆ --- ┆ ---               │
│ i64       ┆ str  ┆ i64 ┆ str               │
╞═══════════╪══════╪═════╪═══════════════════╡
│ 1         ┆ Mike ┆ 24  ┆ 2020-01-17        │
└───────────┴──────┴─────┴───────────────────┘
"""