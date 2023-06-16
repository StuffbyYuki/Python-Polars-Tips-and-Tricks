import polars as pl

df = pl.DataFrame({
    'Letter': ['A', 'B', 'C', 'D', 'F', None, 'H', 'I', 'J'],
    'Value': [1.0, 2, 3, None, 5, 6, 7, None, float("nan")]
})
"""
output:
shape: (9, 2)
┌────────┬───────┐
│ Letter ┆ Value │
│ ---    ┆ ---   │
│ str    ┆ f64   │
╞════════╪═══════╡
│ A      ┆ 1.0   │
│ B      ┆ 2.0   │
│ C      ┆ 3.0   │
│ D      ┆ null  │
│ F      ┆ 5.0   │
│ null   ┆ 6.0   │
│ H      ┆ 7.0   │
│ I      ┆ null  │
│ J      ┆ NaN   │
└────────┴───────┘
"""

##### check missing values #####
# print(df.null_count())  # null_count on the whole dataframe
# print(df.select(pl.col('Value').null_count()))   # null_count on a column
"""
output:
shape: (1, 2)
┌────────┬───────┐
│ Letter ┆ Value │
│ ---    ┆ ---   │
│ u32    ┆ u32   │
╞════════╪═══════╡
│ 1      ┆ 2     │
└────────┴───────┘
shape: (1, 1)
┌───────┐
│ Value │
│ ---   │
│ u32   │
╞═══════╡
│ 2     │
└───────┘
"""

print(
    df
    .filter(pl.col('Value').is_nan())
    .shape[0]
)  # for NaN values
"""
output:
1
"""
# or
print(
    df
    .filter(pl.col('Value').is_nan())
    .select(pl.count()) 
)  # for NaN values, a different way
"""
shape: (1, 1)
┌───────┐
│ count │
│ ---   │
│ u32   │
╞═══════╡
│ 2     │
└───────┘
"""


##### Filling missing values #####
# with literal value
print(
    df.select(pl.col('Value').fill_null(100))
)
"""
shape: (9, 1)
┌───────┐
│ Value │
│ ---   │
│ f64   │
╞═══════╡
│ 1.0   │
│ 2.0   │
│ 3.0   │
│ 100.0 │
│ 5.0   │
│ 6.0   │
│ 7.0   │
│ 100.0 │
│ NaN   │
└───────┘
"""

# with forward strategy
print(
    df.select(pl.col('Value').fill_null(strategy='forward'))
)
"""
shape: (9, 1)
┌───────┐
│ Value │
│ ---   │
│ f64   │
╞═══════╡
│ 1.0   │
│ 2.0   │
│ 3.0   │
│ 3.0   │
│ 5.0   │
│ 6.0   │
│ 7.0   │
│ 7.0   │
│ NaN   │
└───────┘
"""

# with backward strategy
print(
    df.select(pl.col('Value').fill_null(strategy='backward'))
)
"""
shape: (9, 1)
┌───────┐
│ Value │
│ ---   │
│ f64   │
╞═══════╡
│ 1.0   │
│ 2.0   │
│ 3.0   │
│ 5.0   │
│ 5.0   │
│ 6.0   │
│ 7.0   │
│ NaN   │
│ NaN   │
└───────┘
"""

# with expressions
print(
    df.select(pl.col('Value').fill_null(
        (pl.col('Value').max() - pl.col('Value').min()) * 10
    ))
)  
"""
shape: (9, 1)
┌───────┐
│ Value │
│ ---   │
│ f64   │
╞═══════╡
│ 1.0   │
│ 2.0   │
│ 3.0   │
│ 60.0  │
│ 5.0   │
│ 6.0   │
│ 7.0   │
│ 60.0  │
│ NaN   │
└───────┘
"""


##### Drop missing values #####
# drop nulls values
print(df.drop_nulls())
"""
shape: (6, 2)
┌────────┬───────┐
│ Letter ┆ Value │
│ ---    ┆ ---   │
│ str    ┆ f64   │
╞════════╪═══════╡
│ A      ┆ 1.0   │
│ B      ┆ 2.0   │
│ C      ┆ 3.0   │
│ F      ┆ 5.0   │
│ H      ┆ 7.0   │
│ J      ┆ NaN   │
└────────┴───────┘
"""

# drop nan values, after converting them to nulls
print(df.fill_nan(None).drop_nulls())
"""
shape: (5, 2)
┌────────┬───────┐
│ Letter ┆ Value │
│ ---    ┆ ---   │
│ str    ┆ f64   │
╞════════╪═══════╡
│ A      ┆ 1.0   │
│ B      ┆ 2.0   │
│ C      ┆ 3.0   │
│ F      ┆ 5.0   │
│ H      ┆ 7.0   │
└────────┴───────┘
"""