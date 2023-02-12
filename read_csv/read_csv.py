import polars as pl

##### using read_csv #####
df = pl.read_csv('sample_data.csv')
print(df)

'''
shape: (3, 4)
┌───────────┬───────┬─────┬───────────────────┐
│ studentId ┆ Name  ┆ Age ┆ FirstEnrolledDate │
│ ---       ┆ ---   ┆ --- ┆ ---               │
│ i64       ┆ str   ┆ i64 ┆ str               │
╞═══════════╪═══════╪═════╪═══════════════════╡
│ 1         ┆ Mike  ┆ 24  ┆ 2020-01-17        │
│ 2         ┆ Sarah ┆ 33  ┆ 2021-07-23        │
│ 3         ┆ John  ┆ 19  ┆ 2022-12-20        │
└───────────┴───────┴─────┴───────────────────┘
'''

# with parse_date
df = pl.read_csv('sample_data.csv', parse_dates=True)
print(df)
'''
shape: (3, 4)
┌───────────┬───────┬─────┬───────────────────┐
│ studentId ┆ Name  ┆ Age ┆ FirstEnrolledDate │
│ ---       ┆ ---   ┆ --- ┆ ---               │
│ i64       ┆ str   ┆ i64 ┆ date              │
╞═══════════╪═══════╪═════╪═══════════════════╡
│ 1         ┆ Mike  ┆ 24  ┆ 2020-01-17        │
│ 2         ┆ Sarah ┆ 33  ┆ 2021-07-23        │
│ 3         ┆ John  ┆ 19  ┆ 2022-12-20        │
└───────────┴───────┴─────┴───────────────────┘
'''

# with a new date type for a column
df = pl.read_csv('sample_data.csv', parse_dates=True).with_columns(pl.col('Age').cast(pl.Int32))
print(df)
'''
shape: (3, 4)
┌───────────┬───────┬─────┬───────────────────┐
│ studentId ┆ Name  ┆ Age ┆ FirstEnrolledDate │
│ ---       ┆ ---   ┆ --- ┆ ---               │
│ i64       ┆ str   ┆ i32 ┆ date              │
╞═══════════╪═══════╪═════╪═══════════════════╡
│ 1         ┆ Mike  ┆ 24  ┆ 2020-01-17        │
│ 2         ┆ Sarah ┆ 33  ┆ 2021-07-23        │
│ 3         ┆ John  ┆ 19  ┆ 2022-12-20        │
└───────────┴───────┴─────┴───────────────────┘
'''

##### using scan_csv #####
q = pl.scan_csv('sample_data.csv')
print(q.collect())
'''
shape: (3, 4)
┌───────────┬───────┬─────┬───────────────────┐
│ studentId ┆ Name  ┆ Age ┆ FirstEnrolledDate │
│ ---       ┆ ---   ┆ --- ┆ ---               │
│ i64       ┆ str   ┆ i64 ┆ str               │
╞═══════════╪═══════╪═════╪═══════════════════╡
│ 1         ┆ Mike  ┆ 24  ┆ 2020-01-17        │
│ 2         ┆ Sarah ┆ 33  ┆ 2021-07-23        │
│ 3         ┆ John  ┆ 19  ┆ 2022-12-20        │
└───────────┴───────┴─────┴───────────────────┘
'''