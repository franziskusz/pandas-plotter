import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
from collections import OrderedDict

def fill_csv_dicts(path, dict):
    for file in os.listdir(path):
        if file.endswith(".csv"):
            dict[file.replace(".csv", "")] = pd.read_csv(os.path.join(path, file), header=[0], index_col=[0])
    return dict

def make_ordered_dict(keys, dict, ordered_dict):
    for key in keys:
        val = dict[key]
        ordered_dict[key] = val
        del dict[key]
    return ordered_dict



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
    unified_views_df.drop('second', axis=1).plot(subplots=True, title=title)
    #unified_views_df.plot(subplots=True, title=title)
    plt.show()

def dataframe_difference(df1, df2):
    # Create a mask to identify rows with differences
    # Use these lines, when rows without difference should be ignored
    #mask = (abs(df1 - df2)).any(axis=1)
    # Return the rows with differences
    #return abs(df1[mask] - df2[mask])

    return abs(df1 - df2)

#this was supposed to calculate mean over max value of a specific column given as df.max(axis='column_name')
#instead it returned a list of those values of all columns
#unexpected but ok and was adjusted to do exactly that
def max_mean(df_list):
    max_list = []
    for df in df_list:
        max_list.append(df.max())
    max_mean = sum(max_list) / len(max_list)

    return max_mean

def mean_mean(df_list):
    mean_list = []
    for df in df_list:
        mean_list.append(df.mean())
    mean_mean = sum(mean_list) / len(mean_list)

    return mean_mean

def sum_mean(df_list):
    sum_list = []
    for df in df_list:
        sum_list.append(df.sum())
    sum_mean = sum(sum_list) / len(sum_list)

    return sum_mean

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

    #ordered dicts because the order matters
    #since python 3.7 regular dicts are officialy ordered as well
    #but for intent signaling and backwards compatibility OrderedDict was chosen here
    #also this enables the enhanced popitem() method with FIFO option
    rust_godot_csv_dict_ordered = OrderedDict()
    rust_process_csv_dict_ordered = OrderedDict()
    gds_godot_csv_dict_ordered = OrderedDict()
    gds_process_csv_dict_ordered = OrderedDict()

    #lists to save the unified dataframes for calculating the average of multiple runs
    unified_rust_views_seconds_list = []
    unified_gds_views_seconds_list = []
    diff_df_list = []

    #iterating through the csv files in the given directories
    rust_godot_csv_dict = fill_csv_dicts(rust_godot_dir, rust_godot_csv_dict)
    rust_process_csv_dict = fill_csv_dicts(rust_process_dir, rust_process_csv_dict)
    gds_godot_csv_dict = fill_csv_dicts(gds_godot_dir, gds_godot_csv_dict)
    gds_process_csv_dict = fill_csv_dicts(gds_process_dir, gds_process_csv_dict)

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
    #unordered dict items are deleted to free some space
    rust_godot_csv_dict_ordered = make_ordered_dict(rust_godot_keys, rust_godot_csv_dict, rust_godot_csv_dict_ordered)
    rust_process_csv_dict_ordered = make_ordered_dict(rust_process_keys, rust_process_csv_dict, rust_process_csv_dict_ordered)
    gds_godot_csv_dict_ordered = make_ordered_dict(gds_godot_keys, gds_godot_csv_dict, gds_godot_csv_dict_ordered)
    gds_process_csv_dict_ordered = make_ordered_dict(gds_process_keys, gds_process_csv_dict, gds_process_csv_dict_ordered)

    #debug
    #print(rust_godot_keys)
    #print(rust_process_keys)
    #print(gds_godot_keys)
    #print(gds_process_keys)

    #debug
    #print(rust_godot_csv_dict_ordered)
    #print(rust_process_csv_dict_ordered)
    #print(gds_godot_csv_dict_ordered)
    #print(gds_process_csv_dict_ordered)

    #if ONE dictionary is empty while accessed by popitem() the whole rest of try block will be skipped
    #so this is assuming, that all the folders contain the same amount of files
    #even if the first dicts have more items than the rest, the block is aborted before the unified lists are affected
    #this could be changed partially by creating the diff dataframe after the calculation of the averages over the rust / gds dataframes
    #but using different numbers of runs per language does not seem to serve a scientific purpose
    count = 0
    while True:
        try:
            count += 1

            #optional: use popitem(last=False) for First In First Out instead
            rust_godot_key, rust_godot_df = rust_godot_csv_dict_ordered.popitem()
            rust_process_key, rust_process_df = rust_process_csv_dict_ordered.popitem()
            gds_godot_key, gds_godot_df = gds_godot_csv_dict_ordered.popitem()
            gds_process_key, gds_process_df = gds_process_csv_dict_ordered.popitem()
            #print(rust_godot_df)
            #print(rust_process_df)

            unified_rust_views = unify_views(rust_godot_df, rust_process_df)
            unified_gds_views = unify_views(gds_godot_df, gds_process_df)

            #delete all rows, where the value in the 'seconds' column is null
            unified_rust_views_filtered = unified_rust_views.dropna(subset=["second"])
            unified_gds_views_filtered = unified_gds_views.dropna(subset=["second"])

            #make seconds column the new index
            rust_seconds = unified_rust_views_filtered["second"]
            unified_rust_views_by_seconds = unified_rust_views_filtered.set_index(rust_seconds)
            unified_rust_views_by_seconds.drop(columns="second")
            #print("rust unified") #debug
            #print(unified_rust_views_by_seconds)

            gds_seconds = unified_gds_views_filtered["second"]
            unified_gds_views_by_seconds = unified_gds_views_filtered.set_index(gds_seconds)
            unified_gds_views_by_seconds.drop(columns="second")
            #print("gds unified") #debug
            #print(unified_gds_views_by_seconds)

            #calculate the difference between rust and godot
            diff_var_rust = unified_rust_views_by_seconds
            diff_var_gds = unified_gds_views_by_seconds
            diff_df = dataframe_difference(diff_var_rust, diff_var_gds)

            #put the unified rust and godot dataframes and the difference dataframe in their corresponding lists
            unified_rust_views_seconds_list.append(unified_rust_views_by_seconds)
            unified_gds_views_seconds_list.append(unified_gds_views_by_seconds)
            diff_df_list.append(diff_df)

        except KeyError:
            print("In iteration nr "+str(count)+" an empty csv dictionary was found.")
            print("Arithmetic mean is calculated on " +str(count-1)+" unified csv files per language.")
            print("Stopping the process of merging csv files and starting to plot.")
            break


    print("happy plotting!\n")

    #print(unified_rust_views_seconds_list) #debug

    #calculating the arithmetic mean of the lists
    mean_rust = pd.concat(unified_rust_views_seconds_list).groupby(level=0).mean()
    mean_gds = pd.concat(unified_gds_views_seconds_list).groupby(level=0).mean()
    mean_diff = pd.concat(diff_df_list).groupby(level=0).mean()

    #print(unified_rust_views_seconds_list)



    rust_max_mean = max_mean(unified_rust_views_seconds_list)
    print("rust max value mean\n" + rust_max_mean.to_string()+"\n")

    gds_max_mean = max_mean(unified_gds_views_seconds_list)
    print("gds max value mean\n" + gds_max_mean.to_string()+"\n")

    diff_max_mean = max_mean(diff_df_list)
    diff_max_mean = diff_max_mean.drop(['memory usage', 'virtual memory usage', 'second', 'mobs_spawned'])
    print("diff max value mean\n" + diff_max_mean.to_string()+"\n")

    diff_mean_mean = mean_mean(diff_df_list)
    diff_mean_mean = diff_mean_mean.drop(['read bytes', 'written bytes', 'second', 'mobs_spawned', 'hits'])
    print("diff mean value mean\n" + diff_mean_mean.to_string()+"\n")

    diff_sum_mean = sum_mean(diff_df_list)
    diff_sum_mean = diff_sum_mean.drop(['cpu usage', 'read bytes', 'written bytes', 'second', 'mobs_spawned', 'hits'])
    print("diff sum mean\n" + diff_sum_mean.to_string()+"\n")



    #debug
    #print("average rust")
    #print(average_rust)
    #print("average gds")
    #print(average_gds)
    #print("average diff")
    #print(average_diff)

    #plot the averages
    #plot_unified_views_df(mean_rust, "rust")
    #plot_unified_views_df(mean_gds, "godot")
    #plot_unified_views_df(mean_diff, "difference rust - gds")



if __name__ == "__main__":
    main()
