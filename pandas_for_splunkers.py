#!/usr/bin/env python3

import pandas as pd
import sys
from pathlib import Path

main_df = pd.DataFrame()
data_frame_inventory = {}

def clean_list(input):
    dirty_input=input.split(",")
    clean_input = [field.strip() for field in dirty_input]
    return clean_input

def clean_dict(fields,aggregate_functions):
    agg_dict = {item: aggregate_functions for item in fields}
    print(agg_dict)
    return agg_dict

def print_fields(df):
    column_list=list(df.columns)
    print(column_list)

def get_file_extension(file_name):
    path = Path(str(file_name))
    ext = path.suffix
    return ext

    
def load_data(*args):
    global main_df
    if len(args) == 0:
        data_file = input(f"Enter path of data file: ").strip('"')
    else:
        data_file = args[0]

    file_type = get_file_extension(data_file)
   

    if file_type.lower() == ".parquet":
        main_df = pd.read_parquet(data_file)

    if file_type.lower() == ".csv":
        main_df = pd.read_csv(data_file)


def save_dataframe(new_df,df_name):
    data_frame_inventory[df_name] = new_df

def preview_data(df):
    def_records = 20
    def_records = int(input(f"Number of records to preview [{def_records}]: ") or def_records) 
    print(df.head(def_records))

def reduced_fields_data(df):
    global main_df
    print_fields(df)
    fields = input("Type fields of Interest: ")
    if not fields or fields=='*': 
        fields = list(df.columns)
    else:
        fields = clean_list(fields)

    
    try:
        main_df = df[fields]
    except KeyError as e:
        print(f"Invalid Column: {e}, Try Again.")
        reduced_fields_data(df)
    

def aggregate_data(df):
    print_fields(df)
    pivot_fields = input("Comma Delimited List of Fields Aggregate by (stats by x,y,z) : ") or  str(df.columns)
    agg_fields = input("Comma Delimited List of Fields to Aggregate (stats sum(x) min(y) max(z) ): ") or str(df.columns)
    agg_functions = input("Common Aggregation Functions [sum,min,max,mean,std,count,size,average]:") or "count"
    pivot_fields = clean_list(pivot_fields)
    agg_fields = clean_list(agg_fields)
    agg_functions = clean_list(agg_functions)
    agg_dict = clean_dict(agg_fields,agg_functions)
    try:
        agg_df = df.groupby(pivot_fields).agg(agg_dict).reset_index()
        df_name_for_inv = f"{pivot_fields}-{agg_fields}-{agg_functions}"
        df_name_for_inv = df_name_for_inv.replace('[','')
        df_name_for_inv = df_name_for_inv.replace(']','')
        df_name_for_inv = df_name_for_inv.replace(',','_')
        df_name_for_inv = df_name_for_inv.replace('\'','')
        df_name_for_inv = df_name_for_inv.replace(' ','')
        print(f"Dataframe saved as {df_name_for_inv}")
        save_dataframe(agg_df, f"{df_name_for_inv}")

    except KeyError as e:
        print(f"Invalid Column: {e}, Try Again.")
        aggregate_data(df)

def query_data(df):
    print_fields(df)
    query = input("Where clause [mm, returns to main menu]: ")
    if query == "mm":
        pass
    else:
        try:
            print(df.query(query))
            query_data(df)
        except (ValueError,SyntaxError) as e:
            print(f"Error {e}, Try Again.")
            query_data(df)

def list_dataframe():
    counter = 0
    df_keys = {}
    for key in data_frame_inventory.keys():
        df_keys[counter] = key
        counter += 1
    
    for key,value in df_keys.items():
        print(f"Dataframe {key}: {value}")

    user_input = int(input("Select Dataframe number to Load: "))
    print(df_keys)
    name = df_keys.get(user_input, 'Key not found')
    load_dataframe_from_inventory(name)
    




def load_dataframe_from_inventory(key):
    global main_df
    main_df = data_frame_inventory[key]
    return main_df

def main():
    max_rows = 100
    max_cols = 30
    max_disp_width = 500
    max_col_width = 50
    
    while True:
        print("\nMenu:")
        print("1. Load Data  (index/inputlookup)")
        print("2. Preview Data (head)")
        print("3. Select Fields (fields)")
        print("4. Aggregate by Fields (stats)")
        print("5. Filter (where)")
        print("----------------------------------")
        print("i. Dataframe Inventory")
        print("q. Exit")
        print("s. Settings")

        choice = input("Enter your choice: ")

        if choice == '1':
            load_data()
        elif choice == '2':
            preview_data(main_df)
        elif choice == '3':
            reduced_fields_data(main_df)
        elif choice == '4':
            aggregate_data(main_df)
        elif choice == '5':
            query_data(main_df)
        elif choice == 'i':
            list_dataframe()
        elif choice == 'q':
            print("Exiting the program. Goodbye!")
            break
        elif choice == 's':
            max_rows = input(f"Enter max number of rows [{max_rows}]: ") or max_rows
            max_cols = input(f"Enter max number of columns [{max_cols}]: ") or max_cols
            max_disp_width = input(f"Enter display width size [{max_disp_width}]: ") or max_disp_width
            max_col_width = input(f"Enter display width size [{max_col_width}]: ") or max_col_width
            try:
                if isinstance(max_rows, int):
                    print(max_rows, type(max_rows))
                    pd.set_option('display.max_rows', int(max_rows))
                else:
                    pd.set_option('display.max_rows', None)

                if isinstance(max_cols, int):
                    pd.set_option('display.max_columns', max_cols)
                else:
                    pd.set_option('display.max_columns', None)

                if isinstance(max_disp_width, int):
                    pd.set_option('display.width',max_disp_width)
                else:
                    pd.set_option('display.width', None)
                
                if isinstance(max_col_width, int):
                    pd.set_option('display.max_colwidth',max_col_width)
                else:
                    pd.set_option('display.max_colwidth', None)

            except ValueError as e:
                pd.set_option('display.max_rows', int(max_rows))
                pd.set_option('display.max_columns', int(max_cols))
                pd.set_option('display.width',int(max_disp_width))
                pd.set_option('display.max_colwidth',int(max_col_width))
                

            
        else:
            print("Invalid choice. Please enter a number between 1 and 5, q, or s.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(f"Loading: {sys.argv[1]}")
        load_data(sys.argv[1])
    else:
        pass
    
    main()