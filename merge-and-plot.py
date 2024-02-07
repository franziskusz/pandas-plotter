import pandas as pd
import numpy as np
import sys

tolerance = 0

def unify_views(godot_df, process_df):
    #create timestamp in micro seconds series with range from monitor_df
    #create new unified df with series as index
    #join/merge godot_df and monitor df onto new df
    # return new df
    pass

def plot_unified_views_df(unified_views_df):
    #plot
    #save plot
    pass

def unify_languages(unified_views_rust_df, unified_views_gds_df):
    #create new df with seconds column as index
    #return new df
    pass

def calculate_language_difference(unified_languages_df):
    #calculate performance differences between the languages
    #return some statistics
    pass

def plot_unified_languages(unified_languages_df):
    #plot
    #save plot
    pass

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def dataframe_difference(df1, df2):
    # Create a mask to identify rows with differences
    # Use these lines, when rows without difference should be ignored
    #mask = (abs(df1 - df2)).any(axis=1)
    # Return the rows with differences
    #return abs(df1[mask] - df2[mask])

    return abs(df1 - df2)

def difference_tolerance(diff, tolerance):
    # legacy: use lambda to replace
    #diff_bool = diff.all().apply(lambda x: True if x < tolerance else False)

    # convert dataframe to bool according to chosen tolerance
    diff_bool = diff.applymap(num_to_bool)
    # Optional: remove all rows, that contained empty fields
    #diff_bool.dropna(inplace=True)
    return diff_bool

def num_to_bool(value):
    global tolerance

    if value <= tolerance:
        return True
    elif value > tolerance:
        return False
    else:
        return "NaN"

def statistics(bool):
    pass


def main():

    rust_godot_df = pd.read_csv(sys.argv[1], header=[0], index_col=[0], dtype='int64')
    rust_process_df = pd.read_csv(sys.argv[2], header=[0], index_col=[0])
    gds_godot_df = pd.read_csv(sys.argv[3], header=[0], index_col=[0], dtype='int64')
    gds_process_df = pd.read_csv(sys.argv[4], header=[0], index_col=[0])
    # some Variables of importance for adjusting the files to each other
    rust_godot_columns=rust_godot_df.shape[1]
    rust_godot_rows=rust_godot_df.shape[0]
    rust_process_columns=rust_process_df.shape[1]
    rust_process_rows=rust_process_df.shape[0]
    gds_godot_columns=gds_godot_df.shape[1]
    gds_godot_rows=gds_godot_df.shape[0]
    gds_process_columns=gds_process_df.shape[1]
    gds_process_rows=gds_process_df.shape[0]

    print("happy plotting!")



if __name__ == "__main__":
    main()
