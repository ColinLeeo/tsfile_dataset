import posixpath
import config
import fsspec
import re
import os
import json


timebench_metadata = {}


def add_metadata(item_id, file_name, start_time, end_time):
    timebench_metadata[item_id] = {
        "file_name": file_name,
        "start_time": start_time,
        "end_time": end_time,
    }


def load_from_disk(dataset_path: str, keep_in_memory: Optional[bool] = None):
    fs: fsspec.AbstractFileSystem
    if not fs.exists(dataset_path):
        raise FileNotFoundError(f"Directory {dataset_path} not found")
    if not fs.exists(config.DATASET_INFO_FILENAME):
        raise FileNotFoundError(
            f"File {config.DATASET_INFO_FILENAME} not found in {dataset_path}"
        )
    dataset_description = []
    tsfile_path = []
    for filename in os.listdir(dataset_path):
        match = re.fullmatch(r"timebench_(\d+)\.json", filename)
        if match:
            number = match.group(1)
            dataset_description.append(filename)
            tsfile_path.append(f"timebench_{number}.tsfile")

    for index, filename in zip(dataset_description):
        with fs.open(filename, "r") as f:
            item_desc = json.load(f)
            for itemid, time_range in item_desc.items():
                add_metadata(
                    itemid,
                    tsfile_path[index],
                    time_range["start_time"],
                    time_range["end_time"],
                )
    
