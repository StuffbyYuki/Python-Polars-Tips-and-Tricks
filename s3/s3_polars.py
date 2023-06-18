import polars as pl
import pyarrow.dataset as ds
import s3fs
from config import BUCKET_NAME

# set up 
fs = s3fs.S3FileSystem(profile='s3_full_access')

# read parquet
dataset = ds.dataset(f"s3://{BUCKET_NAME}/order_extract.parquet", filesystem=fs, format='parquet')
df_parquet = pl.scan_pyarrow_dataset(dataset)
print(df_parquet.collect().head())

# read parquet 2 
with fs.open(f'{BUCKET_NAME}/order_extract.parquet', mode='rb') as f:
    print(pl.read_parquet(f).head())
          
# read csv
dataset = ds.dataset(f"s3://{BUCKET_NAME}/order_extract.csv", filesystem=fs, format='csv')
df_csv = pl.scan_pyarrow_dataset(dataset)
print(df_csv.collect().head())

# read csv 2 
with fs.open(f'{BUCKET_NAME}/order_extract.csv', mode='rb') as f:
    print(pl.read_csv(f).head())

# prep df
df = pl.DataFrame({
    'ID': [1,2,3,4],
    'Direction': ['up', 'down', 'right', 'left']
})

# write parquet
with fs.open(f'{BUCKET_NAME}/direction.parquet', mode='wb') as f:
    df.write_parquet(f)

# write csv
with fs.open(f'{BUCKET_NAME}/direction.csv', mode='wb') as f:
    df.write_csv(f)
