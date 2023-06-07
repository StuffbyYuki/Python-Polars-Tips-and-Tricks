import polars as pl
import time

file_path = '../duckdb_vs_polars/data/2021_Yellow_Taxi_Trip_Data.csv' 

def select_n_filter_columns(df):
    return (
        df
        .select(pl.col('VendorID', 'tpep_pickup_datetime', 'total_amount', 'tolls_amount', 'payment_type', 'tip_amount', 'fare_amount'))
        .filter(pl.col('total_amount') > 100)
    )

def some_groupby_agg(df):
    return (
        df
        .groupby('VendorID', 'payment_type')
        .agg(
            pl.col('tip_amount').mean().alias('avg_tip_amount')
        )
    )

def some_window_func(df):
    return (
        df
        # .filter(pl.col('VendorID')==1)
        .select(
            pl.col('VendorID'),
            pl.col('payment_type'),
            pl.col('tpep_pickup_datetime'),
            pl.col('tip_amount'),
            pl.col('fare_amount').mean().over('payment_type').alias('mean_fare_amt_per_payment_type'),
        )
        .with_columns(
            pl.col('mean_fare_amt_per_payment_type').rank('dense', descending=True).over('VendorID').alias('dense_rank')
        )
    )

def write_to_csv(df):
    output_file_name = 'output.csv'
    if type(df) == pl.LazyFrame:
        df.collect().write_csv(output_file_name)
        return
    
    return df.write_csv(output_file_name)

def test_performance(func_to_test):
    """
    performance test of one function - lazyframe vs dataframe
    """

    def exec_func_lazyframe():
        """
        when write_to_csv func, don't collect()
        """
        if func_to_test.__name__ == 'write_to_csv':
            return (
                pl.scan_csv(file_path)
                .pipe(func_to_test)
            )
        return (
                pl.scan_csv(file_path)
                .pipe(func_to_test)
                .collect()
            )

    ##### lazyframe #####
    start = time.time()
    exec_func_lazyframe()
    end = time.time()
    time_in_s = round(end - start, 2)
    print(func_to_test.__name__)
    print(f'-> LazyFrame took {time_in_s} seconds')

    ##### dataframe #####
    start = time.time()
    df = (
        pl.read_csv(file_path)
        .pipe(func_to_test)
    )
    end = time.time()
    time_in_s = round(end - start, 2)
    print(f'-> DataFrame took {time_in_s} seconds')

def test_performance_all():
    """
    test performance for all functions used at once
    """
    ##### lazyframe #####
    start = time.time()
    lazy_df = pl.scan_csv(file_path)

    lazy_df = (
        lazy_df
        .pipe(select_n_filter_columns)
        .pipe(some_window_func)
        .pipe(some_groupby_agg)
        .pipe(write_to_csv)
    )
    end = time.time()
    time_in_s = round(end - start, 2)
    print(f'-> LazyFrame took {time_in_s} seconds')

    ##### dataframe #####
    start = time.time()
    df = pl.read_csv(file_path)
    df = (
        df
        .pipe(select_n_filter_columns)
        .pipe(some_window_func)
        .pipe(some_groupby_agg)
        .pipe(write_to_csv)
    )
    end = time.time()
    time_in_s = round(end - start, 2)
    print(f'-> DataFrame took {time_in_s} seconds')

def visualize_result():
    return

def main():

    print('1. Testing performance - a function at a time')
    test_performance(select_n_filter_columns)
    test_performance(some_window_func)
    test_performance(some_groupby_agg)
    test_performance(write_to_csv)

    print('\n2. Testing performance - all functions at once')
    test_performance_all()

if __name__ == '__main__':
    main()
