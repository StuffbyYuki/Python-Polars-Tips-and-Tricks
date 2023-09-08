import polars as pl

DATES_FILE_NAME = 'dates.csv'
DATETIMES_FILE_NAME = 'datetimes.csv'

dates_df = pl.read_csv(DATES_FILE_NAME)
datetimes_df = pl.read_csv(DATETIMES_FILE_NAME)
print(dates_df, '\n', datetimes_df)  # check original data type, which is str

# convert string to date
# 1. to_date and strptime
a = (
    pl.read_csv(DATES_FILE_NAME)
    .with_columns(
        pl.col('date').str.to_date(format='%Y-%m-%d').alias('to_date'),
        pl.col('date').str.strptime(pl.Date, format='%Y-%m-%d').alias('strptime'),
    )
)
print(a)  # check data type

# 2. try_parse_dates when reading
b = (
    pl.read_csv(DATES_FILE_NAME, try_parse_dates=True)
)
print(b)  # check data type

# convert string to datetime / time
# 1. strptime - datetime and time
c = (
    pl.read_csv(DATETIMES_FILE_NAME)
    .with_columns(
        pl.col('datetime').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S').alias('strptime_datetime'),
        pl.col('datetime').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S').alias('strptime_time'),
    )
)
print(c)

# 2. try_parse_dates
d = (
    pl.read_csv(DATETIMES_FILE_NAME, try_parse_dates=True)
)
print(d)
