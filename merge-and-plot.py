import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys

tolerance = 0

def unify_views(godot_df, process_df):
    #original idea: create df with all timestamps between min and max in micros
    #join the other two dfs onto that
    #PROBLEM: process crashes due to memory overload
    #process_timestamps = process_df.index
    #print(process_timestamps)
    #timestamp_min = process_timestamps.min()
    #timestamp_max = process_timestamps.max()
    #print(str(timestamp_min) + " " + str(timestamp_max))
    #all_timestamps = pd.Series(range(timestamp_min, timestamp_max, 1))
    #print(all_timestamps)#debug
    #unified_views_df=pd.DataFrame(index=all_timestamps)
    #print(unified_views_df)#debug
    #join_list = [godot_df, process_df]
    #unified_views_df.join(join_list, how="left")
    #print(unified_views_df)#debug

    #simplified approach: divide indexes by 1000000 and join dfs directly
    #at cost of subsecond precision
    float_process_seconds_index = process_df.index / 1000000
    process_seconds_index = float_process_seconds_index.astype("int")
    float_godot_seconds_index = godot_df.index / 1000000
    godot_seconds_index = float_godot_seconds_index.astype("int")
    #print(process_seconds_index)#debug
    #print(godot_seconds_index)#debug

    godot_new = godot_df.set_index(godot_seconds_index)
    process_new = process_df.set_index(process_seconds_index)
    #print(godot_new) #debug
    #print(process_new) #debug
    unified_views_df=process_new.join(godot_new, how="left")
    #print(unified_views_df) #debug

    #create timestamp in micro seconds series with range from monitor_df
    #create new unified df with series as index
    #join/merge godot_df and monitor df onto new df
    # return new df
    return unified_views_df

def plot_unified_views_df(unified_views_df, title):
    unified_views_df.plot(subplots=True, title=title)
    plt.show()

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

    print("happy plotting!")

    unified_rust_views = unify_views(rust_godot_df, rust_process_df)
    #plot_unified_views_df(unified_rust_views, "rust")

    unified_gds_views = unify_views(gds_godot_df, gds_process_df)
    #plot_unified_views_df(unified_gds_views, "godot")

    unified_rust_views_filtered = unified_rust_views.dropna(subset=["second"])
    print(unified_rust_views_filtered)
    unified_gds_views_filtered = unified_gds_views.dropna(subset=["second"])
    print(unified_gds_views_filtered)

    rust_seconds = unified_rust_views_filtered["second"]
    unified_rust_views_by_seconds = unified_rust_views_filtered.set_index(rust_seconds)
    unified_rust_views_by_seconds.drop(columns="second")
    print(unified_rust_views_by_seconds)

    gds_seconds = unified_gds_views_filtered["second"]
    unified_gds_views_by_seconds = unified_gds_views_filtered.set_index(gds_seconds)
    unified_gds_views_by_seconds.drop(columns="second")
    print(unified_gds_views_by_seconds)

    diff_df = dataframe_difference(unified_rust_views_by_seconds, unified_gds_views_by_seconds)

    plot_unified_views_df(diff_df, "difference rust - gds")




if __name__ == "__main__":
    main()
