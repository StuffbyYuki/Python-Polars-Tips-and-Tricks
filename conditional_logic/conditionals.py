import polars as pl

df = pl.LazyFrame(
    {'Numbers': [1,2,3,4,5]}
)

print(df.collect().head())
'''
output:
shape: (5, 1)
┌─────────┐
│ Numbers │
│ ---     │
│ i64     │
╞═════════╡
│ 1       │
│ 2       │
│ 3       │
│ 4       │
│ 5       │
└─────────┘
'''

df = (
    df
    .with_columns(
        pl.when(pl.col('Numbers')==1)
        .then('Best')
        .when(pl.col('Numbers')==2)
        .then('Second Best')
        .otherwise('Not Good')
        .alias('Rank')
    )
    
)

print(df.collect().head())
'''
output:
shape: (5, 2)
┌─────────┬─────────────┐
│ Numbers ┆ Rank        │
│ ---     ┆ ---         │
│ i64     ┆ str         │
╞═════════╪═════════════╡
│ 1       ┆ Best        │
│ 2       ┆ Second Best │
│ 3       ┆ Not Good    │
│ 4       ┆ Not Good    │
│ 5       ┆ Not Good    │
└─────────┴─────────────┘
'''