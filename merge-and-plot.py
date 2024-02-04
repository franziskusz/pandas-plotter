import pandas as pd
import numpy as np
import sys

tolerance = 0

def unify_views(godot_df, monitor_df):
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

def plot unified_languages(unified_languages_df):
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

    dodge_rust_df = pd.read_csv(sys.argv[1], header=[0], index_col=[0], dtype='int64')
    dodge_rust_process_df = pd.read_csv(sys.argv[2], header=[0], index_col=[0], dtype='int64')
    dodge_gds_df = pd.read_csv(sys.argv[3], header=[0], index_col=[0], dtype='int64')
    dodge_gds_process_df = pd.read_csv(sys.argv[4], header=[0], index_col=[0], dtype='int64')
    # some Variables of importance for adjusting the files to each other
    dodge_rust_df=dodge_rust_df.shape[1]
    dodge_rust_df=dodge_rust_df.shape[0]
    dodge_rust_process_df=dodge_rust_process_df.shape[1]
    dodge_rust_process_df=dodge_rust_process_df.shape[0]
    dodge_gds_df=dodge_gds_df.shape[1]
    dodge_gds_df=dodge_gds_df.shape[0]
    dodge_gds_process_df=dodge_gds_process_df.shape[1]
    dodge_gds_process_df=dodge_gds_process_df.shape[0]




if __name__ == "__main__":
    main()
