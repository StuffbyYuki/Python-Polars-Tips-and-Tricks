import polars as pl

# get data
FILE_PATH = "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv"
df = pl.read_csv(FILE_PATH)
print(df.head())

# aggregation over multiple columns
agg_col = (
    df
    .select(
        pl.all().exclude('#', 'Name', 'Type 1', 'Type 2', 'Generation', 'Legendary', 'Total')
    )
    .sum(axis=1)
    .alias('agg over cols')
)
print(agg_col)

# add the calculation above as a new column
# option 1 - with_columns
df_1 = (
    df
    .with_columns(
        agg_col,
        pl.col('HP').mean().over('Type 1').alias('Avg HP Per Type 1')
    )
)
print(df_1.select(pl.col(['Name', 'Type 1', 'Total', 'agg over cols', 'Avg HP Per Type 1']).head()))

# option 2 - select
df_2 = (
    df
    .select(
        pl.col('*'),
        agg_col,
        pl.col('HP').mean().over('Type 1').alias('Avg HP Per Type 1')
    )
)
print(df_2.select(pl.col(['Name', 'Type 1', 'Total', 'agg over cols', 'Avg HP Per Type 1']).head()))
