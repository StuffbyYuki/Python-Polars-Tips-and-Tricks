import polars as pl
import duckdb

# read in data
df_pl = pl.scan_csv('../sample_data.csv').collect()
df_arrow = df_pl.to_arrow()
print(df_arrow)
'''
pyarrow.Table
studentId: int64
Name: large_string
Age: int64
FirstEnrolledDate: large_string
----
studentId: [[1,2,3]]
Name: [["Mike","Sarah","John"]]
Age: [[24,33,19]]
FirstEnrolledDate: [["2020-01-17","2021-07-23","2022-12-20"]]
'''

# filtering in polars 
df_pl_filtered = (
    df_pl
    .filter(pl.col('Name')=='Mike')
)
print(df_pl_filtered)
'''
shape: (1, 4)
┌───────────┬──────┬─────┬───────────────────┐
│ studentId ┆ Name ┆ Age ┆ FirstEnrolledDate │
│ ---       ┆ ---  ┆ --- ┆ ---               │
│ i64       ┆ str  ┆ i64 ┆ str               │
╞═══════════╪══════╪═════╪═══════════════════╡
│ 1         ┆ Mike ┆ 24  ┆ 2020-01-17        │
└───────────┴──────┴─────┴───────────────────┘
'''

# filtering in duckdb
df_duckdb_filtered = duckdb.query("""
  select 
    *
  from df_arrow
  where Name = 'Mike'
  
""")
print(df_duckdb_filtered)
'''
---------------------
--- Relation Tree ---
---------------------
Subquery

---------------------
-- Result Columns  --
---------------------
- studentId (BIGINT)
- Name (VARCHAR)
- Age (BIGINT)
- FirstEnrolledDate (VARCHAR)

---------------------
-- Result Preview  --
---------------------
studentId       Name    Age     FirstEnrolledDate
BIGINT  VARCHAR BIGINT  VARCHAR
[ Rows: 1]
1       Mike    24      2020-01-17
'''

# a window function in polars
df_pl_rank = (
    df_pl
    .with_columns([
        pl.col('FirstEnrolledDate').rank().cast(pl.Int64).alias('EnrolledRank')
    ])
)
print(df_pl_rank)
'''
shape: (3, 5)
┌───────────┬───────┬─────┬───────────────────┬──────────────┐
│ studentId ┆ Name  ┆ Age ┆ FirstEnrolledDate ┆ EnrolledRank │
│ ---       ┆ ---   ┆ --- ┆ ---               ┆ ---          │
│ i64       ┆ str   ┆ i64 ┆ str               ┆ i64          │
╞═══════════╪═══════╪═════╪═══════════════════╪══════════════╡
│ 1         ┆ Mike  ┆ 24  ┆ 2020-01-17        ┆ 1            │
│ 2         ┆ Sarah ┆ 33  ┆ 2021-07-23        ┆ 2            │
│ 3         ┆ John  ┆ 19  ┆ 2022-12-20        ┆ 3            │
└───────────┴───────┴─────┴───────────────────┴──────────────┘
'''


# a window function in duckdb
df_duckdb_rank = duckdb.query("""
  select 
    *,
    rank() over(order by FirstEnrolledDate) as EnrolledRank

  from df_arrow
  
""")
print(df_duckdb_rank)
'''
---------------------
--- Relation Tree ---
---------------------
Subquery

---------------------
-- Result Columns  --
---------------------
- studentId (BIGINT)
- Name (VARCHAR)
- Age (BIGINT)
- FirstEnrolledDate (VARCHAR)
- EnrolledRank (BIGINT)

---------------------
-- Result Preview  --
---------------------
studentId       Name    Age     FirstEnrolledDate       EnrolledRank
BIGINT  VARCHAR BIGINT  VARCHAR BIGINT
[ Rows: 3]
1       Mike    24      2020-01-17      1
2       Sarah   33      2021-07-23      2
3       John    19      2022-12-20      3
'''

# convert duckdb to polars dataframe
duckdb_to_polars = pl.DataFrame(df_duckdb_rank.arrow())
print(duckdb_to_polars)
'''
shape: (3, 5)
┌───────────┬───────┬─────┬───────────────────┬──────────────┐
│ studentId ┆ Name  ┆ Age ┆ FirstEnrolledDate ┆ EnrolledRank │
│ ---       ┆ ---   ┆ --- ┆ ---               ┆ ---          │
│ i64       ┆ str   ┆ i64 ┆ str               ┆ i64          │
╞═══════════╪═══════╪═════╪═══════════════════╪══════════════╡
│ 1         ┆ Mike  ┆ 24  ┆ 2020-01-17        ┆ 1            │
│ 2         ┆ Sarah ┆ 33  ┆ 2021-07-23        ┆ 2            │
│ 3         ┆ John  ┆ 19  ┆ 2022-12-20        ┆ 3            │
└───────────┴───────┴─────┴───────────────────┴──────────────┘
'''