import pandas as pd
from typing import List, Dict
from provlake.utils.args_handler import get_dict, get_list, get_dataset, get_recursive_dicts, get_kg_reference, \
    add_custom_metadata, get_dataset_item


def csv_extraction_function(file_path_or_buffer, dataset_schema_id: str, dataset_id: str) -> dict:
    """default csv extraction"""
    args_list = []
    df = pd.read_csv(file_path_or_buffer)
    non_null_columns = [col for col in df.columns if df.loc[:, col].notna().any()]
    df = df[non_null_columns]

    i = 0
    for index, row in df.iterrows():
        dataset_item = dict(row)
        args_list.append(get_dataset_item(values=get_dict(dataset_item), order=i, dataset_id=dataset_id))
        i += 1

    return {dataset_schema_id: get_list(args_list)}


def city_csv_extraction_function(file_path_or_buffer, dataset_schema_id: str) -> dict:
    """extracts city"""
    args_list = []
    df = pd.read_csv(file_path_or_buffer)
    i = 0
    for index, row in df.iterrows():
        dataset_item = dict(row)
        dataset_item["ID"] = get_kg_reference(dataset_item["ID"])
        args_list.append(get_dataset_item(values=get_dict(dataset_item), order=i))
        i += 1

    return {dataset_schema_id: get_list(args_list)}
