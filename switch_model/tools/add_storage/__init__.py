"""
This package was created by Martin Staadecker
when studying long duration energy storage. It
allows adding storage technologies from a csv file to
the csvs in the inputs folder.
"""
import os

import pandas as pd


def fetch_df(tab_name):
    tab_name_to_gid = {
        "constants": 0,
        "plants": 889129113,
        "costs": 1401952285
    }
    gid = tab_name_to_gid[tab_name]
    sheet_id = "1SJrj039T1T95NLTs964VQnsfZgo2QWCo29x2ireVYcU"
    url = f"https://docs.google.com/spreadsheet/ccc?key={sheet_id}&output=csv&gid={gid}"
    df = pd.read_csv(url, index_col=False) \
        .replace("FALSE", 0) \
        .replace("TRUE", 1)
    return df


def filer_by_scenario(df, column_name):
    scenario = input(f"Which scenario do you want for '{column_name}' (default 0) : ")
    if scenario == "":
        scenario = 0
    scenario = int(scenario)
    df = df[df[column_name] == scenario]
    return df.drop(column_name, axis=1)


def cross_join(df1, df2):
    return df1.assign(key=1).merge(
        df2.assign(key=1),
        on="key"
    ).drop("key", axis=1)


def append_to_input_file(filename, to_add):
    path = os.path.join("inputs", filename)
    df = pd.read_csv(path, index_col=False)
    col = df.columns
    df = pd.concat([df, to_add], ignore_index=True)[col]
    df.to_csv(path, index=False)


def get_gen_constants():
    df = fetch_df("constants")
    df = filer_by_scenario(df, "constant_scenario")
    df = df.set_index("param_name")
    return df.transpose()


def main():
    gen_constants = get_gen_constants()
    gen_plants = fetch_df("plants")
    gen_plants = cross_join(gen_plants, gen_constants)

    append_to_input_file("generation_projects_info.csv", gen_plants)


if __name__ == "__main__":
    main()
