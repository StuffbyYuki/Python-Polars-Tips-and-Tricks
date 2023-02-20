import polars as pl
import pandas as pd
import time
import sys 
sys.path.insert(1, '../')
from config import file_path_10million_rows_csv

FILE_PATH = file_path_10million_rows_csv

def read_polars(file_path):
    '''
    Read a csv with polars
    '''
    df = pl.scan_csv(file_path).collect()
    return df

def read_pandas(file_path):
    '''
    Read a csv file with pandas
    '''
    df = pd.read_csv(file_path)
    return df

def agg_polars(pl_df):
    '''
    take a polars df and execute a few aggregate functions
    '''
    q = (
        pl_df.lazy()
        .groupby('state')
        .agg(
            [
                pl.count().alias('cnt'),
                pl.col('age').mean().alias('mean age'),
                pl.col('age').min().alias('min age'),
                pl.col('age').max().alias('max age')
            ]
        )
        .sort('state')
    )

    output = q.collect()
    
    return output

def agg_pandas(pd_df):
    '''
    take a pandas df and execute a few aggregate functions
    '''
    output = (
        pd_df
        .groupby('state')['age']
        .agg(            
            ['count', 'mean', 'min', 'max'],    
        )
        .sort_values(by=['state'])
    )

    return output

def window_func_polars(pl_df):
    '''
    new column 1 - avg age per state
    new column 2 - age rank within each state
    '''
    q = (
        pl_df.lazy()
        .select(
            [
                'email',
                'first',
                'last',
                'state',
                'age'
            ]
        )
        .with_columns([
            pl.col('age').mean().over('state').alias('avg_age_per_state'),
            pl.col('age').rank(method='dense').over('state').alias('age_rank')
        ])
        .sort('state')
    )

    output = q.collect()

    return output

def window_func_pandas(pd_df):
    '''
    new column 1 - avg age per state
    new column 2 - age rank within each state
    '''
    output = (
        pd_df
        .loc[:, ['email', 'first', 'last', 'state', 'age']]
        .assign(avg_age_per_state=lambda df: df.groupby('state')['age'].transform('mean'))
        .assign(age_rank=lambda df: df.groupby('state')['age'].rank(method='dense'))
        .sort_values(by=['state'])
    )
    
    return output

def inner_join_polars():
    return

def left_outer_join_polars():
    return

def full_outer_join_polars():
    return

def inner_join_pandas():
    return

def left_outer_join_pandas():
    return

def full_outer_join_pandas():
    return

def time_a_function(func, *args):
    '''
    A func to time a function
    '''
    start_time = time.time()
    func(*args)
    end_time = time.time()
    result = round(end_time - start_time, 2)
    return result

def main():

    ##### read a csv file #####
    # print('Reading 10 million rows with 14 columns (1.2GB)...')

    # pl_time = time_a_function(read_polars, FILE_PATH)
    # print(f'Polars took {pl_time} seconds.')

    # pd_time = time_a_function(read_pandas, FILE_PATH)
    # print(f'Pandas took {pd_time} seconds.')


    ##### simple aggregations #####
    # print('Testing performance on aggregations...')
    # pl_df = read_polars(FILE_PATH)
    # pl_time = time_a_function(agg_polars, pl_df)
    # print(f'Polars took {pl_time} seconds.')
    
    # pd_df = read_pandas(FILE_PATH)
    # pd_time = time_a_function(agg_pandas, pd_df)
    # print(f'Pandas took {pd_time} seconds.')


    ##### window functions #####
    print('Testing performance on window functions...')
    pl_df = read_polars(FILE_PATH)
    pl_time = time_a_function(window_func_polars, pl_df)
    print(f'Polars took {pl_time} seconds.')
    
    pd_df = read_pandas(FILE_PATH)
    pd_time = time_a_function(window_func_pandas, pd_df)
    print(f'Pandas took {pd_time} seconds.')
    
    # join operations

if __name__ == '__main__':
    main()

