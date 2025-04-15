from dataset import dataset, tsfile_reader
import os

def test_dataset():
    data = dataset("/home/colin/dev/tmp/result/result")
    result = data.get_item(100)
    data_result = result.get_randm_batch(1024)
    print("data_result:", data_result)

test_dataset()