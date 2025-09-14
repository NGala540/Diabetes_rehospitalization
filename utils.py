import polars as pl

def create_replace_exp(x, map):
    # helper function for "replace_values"
    return pl.col(x).replace_strict(map[x], default=pl.col(x)).alias(x)

# TODO: add logs
def replace_values(df: pl.dataframe, map:dict):
    # replace values in df columns base on given mapping
    temp = [create_replace_exp(col, map) for col in map.keys() if col in df.columns]
    return df.with_columns(temp)

def show_null(df: pl.DataFrame, tbl_rows:int = -1):
    # show nulls with proper formatting
    with pl.Config(tbl_rows=tbl_rows):
        print(df.null_count().transpose(include_header=True).with_columns(null_share=(pl.col("column_0")/df.height)*100))