import pandas as pd
from typing import List, Dict
from provlake.utils.args_handler import get_dict, get_list, get_dataset, get_recursive_dicts, get_kg_reference, \
    add_custom_metadata, get_dataset_item


def csv_extraction_function(file_path, dataset_name) -> dict:
    args_list = []
    df = pd.read_csv(file_path)
    i = 0
    for index, row in df.iterrows():
        args_list.append(get_dataset_item(values=get_dict(dict(row)), order=i))
        i += 1

    return {dataset_name: get_list(args_list)}


def city_csv_extraction_function(file_path, dataset_name) -> dict:
    args_list = []
    df = pd.read_csv(file_path)
    i = 0
    for index, row in df.iterrows():
        dataset_item = dict(row)
        dataset_item["ID"] = get_data_reference_as_is(dataset_item["ID"])
        args_list.append(get_dataset_item(values=get_dict(dataset_item), order=i))
        i += 1

    return {dataset_name: get_list(args_list)}
