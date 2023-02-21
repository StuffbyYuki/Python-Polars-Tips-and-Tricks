import polars as pl

# creating dummy data
data = [[1, 2, 3], ['A', 'B', 'C'], [111, 222, 333]]
df = pl.DataFrame(data, schema=['ID', 'Letter', 'Values'])
print(type(df))

# 1. using .to_series() on .select()
series_1 = df.select('ID').to_series()
print(type(series_1))

# 2. using .to_series() alone, column index specified
series_2 = df.to_series(2)
print(type(series_2))

# 3. just taking a column as a series
series_3 = df['ID']
print(type(series_3))

# .select() itself returns a dataframe
this_is_df = df.select('ID')
print(type(this_is_df))
