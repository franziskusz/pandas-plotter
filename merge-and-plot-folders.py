import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
from collections import OrderedDict

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
    #TODO remove seconds column
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
    #save paths to folders from script invocation to variables
    rust_godot_dir = sys.argv[1]
    rust_process_dir = sys.argv[2]
    gds_godot_dir = sys.argv[3]
    gds_process_dir = sys.argv[4]

    #unordered dicts to save filename and table with listdir() which collects files in arbitrary order
    rust_godot_csv_dict = dict()
    rust_process_csv_dict = dict()
    gds_godot_csv_dict = dict()
    gds_process_csv_dict = dict()

    #ordered dict because the order matters
    rust_godot_csv_dict_ordered = OrderedDict()
    rust_process_csv_dict_ordered = OrderedDict()
    gds_godot_csv_dict_ordered = OrderedDict()
    gds_process_csv_dict_ordered = OrderedDict()

    #lists to save the unified dataframes for calculating the average of multiple runs
    unified_rust_views_seconds_list = []
    unified_gds_views_seconds_list = []
    diff_df_list = []

    #iterating through the csv files in the given directories
    for file in os.listdir(rust_godot_dir):
        if file.endswith(".csv"):
            rust_godot_csv_dict[file.replace(".csv", "")] = pd.read_csv(os.path.join(rust_godot_dir, file), header=[0], index_col=[0], dtype='int64')

    for file in os.listdir(rust_process_dir):
        if file.endswith(".csv"):
            rust_process_csv_dict[file.replace(".csv", "")] = pd.read_csv(os.path.join(rust_process_dir, file), header=[0], index_col=[0])

    for file in os.listdir(gds_godot_dir):
        if file.endswith(".csv"):
            gds_godot_csv_dict[file.replace(".csv", "")] = pd.read_csv(os.path.join(gds_godot_dir, file), header=[0], index_col=[0], dtype='int64')

    for file in os.listdir(gds_process_dir):
        if file.endswith(".csv"):
            gds_process_csv_dict[file.replace(".csv", "")] = pd.read_csv(os.path.join(gds_process_dir, file), header=[0], index_col=[0])

    #sorting by the filenames which should be given some kind of order (timestamp)
    rust_godot_keys = list(rust_godot_csv_dict.keys())
    rust_godot_keys.sort()
    rust_process_keys = list(rust_process_csv_dict.keys())
    rust_process_keys.sort()
    gds_godot_keys = list(gds_godot_csv_dict.keys())
    gds_godot_keys.sort()
    gds_process_keys = list(gds_process_csv_dict.keys())
    gds_process_keys.sort()

    #consuming unordered dicts into the ordered ones
    for key in rust_godot_keys:
        val = rust_godot_csv_dict[key]
        rust_godot_csv_dict_ordered[key] = val
        del rust_godot_csv_dict[key]

    for key in rust_process_keys:
        val = rust_process_csv_dict[key]
        rust_process_csv_dict_ordered[key] = val
        del rust_process_csv_dict[key]

    for key in gds_godot_keys:
        val = gds_godot_csv_dict[key]
        gds_godot_csv_dict_ordered[key] = val
        del gds_godot_csv_dict[key]

    for key in gds_process_keys:
        val = gds_process_csv_dict[key]
        gds_process_csv_dict_ordered[key] = val
        del gds_process_csv_dict[key]

    #debug
    #print(rust_godot_keys)
    #print(rust_process_keys)
    #print(gds_godot_keys)
    #print(gds_process_keys)

    #print(rust_godot_csv_dict_ordered)
    #print(rust_process_csv_dict_ordered)
    #print(gds_godot_csv_dict_ordered)
    #print(gds_process_csv_dict_ordered)

    #while True:
    #    try:
    #        rust_godot_key, rust_godot_df = rust_godot_csv_dict_ordered.popitem()
    #    except KeyError:
    #        print ("dictionary is empty")

    #if ONE dictionary is empty while accessed by popitem() the whole rest of try block will be skipped
    #so this only works as intended, if the folders contain the same amount of files
    while True:
        try:
            rust_godot_key, rust_godot_df = rust_godot_csv_dict_ordered.popitem()
            rust_process_key, rust_process_df = rust_process_csv_dict_ordered.popitem()
            gds_godot_key, gds_godot_df = gds_godot_csv_dict_ordered.popitem()
            gds_process_key, gds_process_df = gds_process_csv_dict_ordered.popitem()
            #print(rust_godot_df)
            #print(rust_process_df)

            unified_rust_views = unify_views(rust_godot_df, rust_process_df)
            unified_gds_views = unify_views(gds_godot_df, gds_process_df)

            unified_rust_views_filtered = unified_rust_views.dropna(subset=["second"])
            unified_gds_views_filtered = unified_gds_views.dropna(subset=["second"])

            rust_seconds = unified_rust_views_filtered["second"]
            unified_rust_views_by_seconds = unified_rust_views_filtered.set_index(rust_seconds)
            unified_rust_views_by_seconds.drop(columns="second")
            print("rust unified")
            print(unified_rust_views_by_seconds)

            gds_seconds = unified_gds_views_filtered["second"]
            unified_gds_views_by_seconds = unified_gds_views_filtered.set_index(gds_seconds)
            unified_gds_views_by_seconds.drop(columns="second")
            print("gds unified")
            print(unified_gds_views_by_seconds)

            diff_var_rust = unified_rust_views_by_seconds
            diff_var_gds = unified_gds_views_by_seconds
            diff_df = dataframe_difference(diff_var_rust, diff_var_gds)

            unified_rust_views_seconds_list.append(unified_rust_views_by_seconds)
            unified_gds_views_seconds_list.append(unified_gds_views_by_seconds)

            diff_df_list.append(diff_df)


        except KeyError:
            print("csv dictionary is empty")
            break


    print("happy plotting!")

    #print(unified_rust_views_seconds_list)

    #calculating the average of the lists
    average_rust = pd.concat(unified_rust_views_seconds_list).groupby(level=0).mean()
    average_gds = pd.concat(unified_gds_views_seconds_list).groupby(level=0).mean()
    average_diff = pd.concat(diff_df_list).groupby(level=0).mean()
    print("average rust")
    print(average_rust)
    print("average gds")
    print(average_gds)
    print("average diff")
    print(average_diff)

    #plot the averages
    plot_unified_views_df(unified_rust_views, "rust")
    plot_unified_views_df(unified_gds_views, "godot")
    plot_unified_views_df(diff_df, "difference rust - gds")



if __name__ == "__main__":
    main()
