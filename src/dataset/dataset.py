from typing import Dict, List, Any
from tsfile_reader_cache import TsFileReaderCahce
from tsfile import TsFileReader
import numpy as np
import fsspec
import json

class tsfile_reader():
    
    def __init__(self, path:str, start:int, end:int):
        self.start = start
        self.end = end
        self.reader = TsFileReader(path)
    def get_randm_batch(self, batch_size:int) -> List[Any]:
        data = [None] * batch_size
        end = self.end if self.end - batch_size < self.start else self.end - batch_size
        start = np.random.randint(self.start, end)
        print(self.reader.get_all_table_schemas())
        end = start + batch_size if start + batch_size < self.end else self.end
        result = self.reader.query_table("timebench", ["value"], start, end)
        i = 0
        while result.next() and i < batch_size:
            data[i] = result.get_value_by_name("value")
            i = i + 1
        return data
        
        


class dataset():
    def __init__(self, dataset_path:str):
        self.dataset_path = dataset_path
        self.dataset_num_per_tsfile = 10_000

    def _get_id(self, tsfile_id:int) -> int:
        return tsfile_id // self.dataset_num_per_tsfile

    def get_item(self, item_id:int) -> tsfile_reader:
        tsfile_id = self._get_id(item_id)
        tsfile_path = f"{self.dataset_path}/timebench_{tsfile_id}.tsfile"
        series_id = item_id % self.dataset_num_per_tsfile
        start_time = None
        end_time = None
        print("tsfile_id type:", type(tsfile_id))
        print("full path:", f"{self.dataset_path}/timebench_{tsfile_id}.json")
        with open(f"{self.dataset_path}/timebench_{tsfile_id}.json", "r", encoding='utf-8') as f:
            item_desc = json.load(f)
            item = list(item_desc.items())[series_id]
            _, time_range = item
            start_time, end_time = time_range
        return tsfile_reader(tsfile_path, int(start_time), int(end_time))



    
