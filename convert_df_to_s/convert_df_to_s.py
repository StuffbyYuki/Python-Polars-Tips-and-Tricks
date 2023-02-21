import polars as pl

# creating dummy data
data = [[1, 2, 3], ['A', 'B', 'C'], [111, 222, 333]]
df = pl.DataFrame(data, schema=['ID', 'Letter', 'Values'])
print(type(df))

# 1. using .to_series() on .select()
series_1 = df.select('ID').to_series()
print(type(series_1))

# 2. just taking a column as a series
series_2 = df['ID']
print(type(series_2))

# .select() itself returns a dataframe
this_is_df = df.select('ID')
print(type(this_is_df))
