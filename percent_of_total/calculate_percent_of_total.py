import polars as pl

df = pl.LazyFrame(
    {
        'Letter': ['A','A','A','B','B','C','D','D','D','D','D','E','F','G'],
        'Value': [1,2,3,4,5,6,7,8,9,10,11,12,13,14]
     }
)

# one way
df1 = (
    df
    .groupby('Letter')
    .agg(
        pl.count().alias('Cnt Per Letter')
    )
    .with_columns(
        (
            (pl.col('Cnt Per Letter') / pl.col('Cnt Per Letter').sum()).round(2) * 100
        )
        .cast(pl.UInt64).alias('Percent of Total')
    )
    .sort('Letter')
)

print(df1.collect())

# another way
df2 = (
    df
    .with_columns(
        pl.col('Value').count().over('Letter').alias('Cnt Per Letter'),
        pl.count().alias('Total Cnt')
    )
    .with_columns(
        (
            (pl.col('Cnt Per Letter') / pl.col('Total Cnt')).round(2) * 100
        )
        .cast(pl.UInt64).alias('Percent of Total')
    )
    .unique('Letter')
    .select(['Letter', 'Cnt Per Letter', 'Percent of Total'])
    .sort('Letter')
)

print(df2.collect())
