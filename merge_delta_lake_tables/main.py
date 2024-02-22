# source dataframe
import polars as pl

df = pl.DataFrame(
    {
        'key': [1, 2, 3],
        'letter': ['a', 'b', 'c'],
        'value': [100, 200, 300]
    }
)
print(df)

# write to a delta lake table
output_table_path = './my_delta_lake_table'

df.write_delta(output_table_path, mode='overwrite')
print('The target Delta Lake table output:\n', pl.read_delta(output_table_path))

# make changes to the source dataframe
df_col_updated_and_row_deleted = (
    df
    .with_columns(
        pl.when(pl.col('letter')=='c')  # update a column 
        .then(pl.lit('d'))
        .otherwise(pl.col('letter'))  
        .alias('letter')
    )
    .filter(pl.col('key') != 1)  # delete a row 
)

df_with_changes = (
    pl.concat(
        [
            df_col_updated_and_row_deleted,
            pl.DataFrame({'key': 4, 'letter': 'd', 'value': 400})  # a new row
        ],
        how='vertical'
    )
)
print(df_with_changes)

# merge changes to the target delta lake table
(
    df_with_changes
    .write_delta(
        output_table_path,
        mode='merge',
        delta_merge_options={
            'predicate': 'source.key = target.key',
            'source_alias': 'source',
            'target_alias': 'target',
        },
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .when_not_matched_by_source_delete()
    .execute()
)  

# check the output
print('Delta lake table output:\n', pl.read_delta(output_table_path))

# everythign in one
import polars as pl

df = pl.DataFrame(
    {
        'key': [1, 2, 3],
        'letter': ['a', 'b', 'c'],
        'value': [100, 200, 300]
    }
)

output_table_path = './my_delta_lake_table'

df.write_delta(output_table_path, mode='overwrite')
print('The target Delta Lake table output:\n', pl.read_delta(output_table_path))

df_col_updated_and_row_deleted = (
    df
    .with_columns(
        pl.when(pl.col('letter')=='c')  # update a column 
        .then(pl.lit('d'))
        .otherwise(pl.col('letter'))  
        .alias('letter')
    )
    .filter(pl.col('key') != 1)  # delete a row 
)

df_with_changes = (
    pl.concat(
        [
            df_col_updated_and_row_deleted,
            pl.DataFrame({'key': 4, 'letter': 'd', 'value': 400})  # a new row
        ],
        how='vertical'
    )
)
print(df_with_changes)

(
    df_with_changes
    .write_delta(
        output_table_path,
        mode='merge',
        delta_merge_options={
            'predicate': 'source.key = target.key',
            'source_alias': 'source',
            'target_alias': 'target',
        },
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .when_not_matched_by_source_delete()
    .execute()
)  

print('Delta lake table output:\n', pl.read_delta(output_table_path))
