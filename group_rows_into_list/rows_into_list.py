import polars as pl

df = pl.LazyFrame(
    {
        'Letter': ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'D'],
        'Value': [1, 2, 3, 4, 5, 6, 7, 8, 9]
    }
)

df = (
    df
    .groupby('Letter')
    .agg(pl.col('Value'))
    .sort('Letter')
)

print(df.fetch())
'''
output:
shape: (4, 2)
┌────────┬───────────┐
│ Letter ┆ Value     │
│ ---    ┆ ---       │
│ str    ┆ list[i64] │
╞════════╪═══════════╡
│ A      ┆ [1, 2, 3] │
│ B      ┆ [4, 5, 6] │
│ C      ┆ [7, 8]    │
│ D      ┆ [9]       │
└────────┴───────────┘
'''