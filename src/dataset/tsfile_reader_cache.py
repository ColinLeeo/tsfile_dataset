from tsfile import TsFileReader

class TsFileReaderCahce:
    def __init__(self, max_size = 100):
        self._cache: dict[str, TsFileReader] = {}
        self.max_size = max_size 
    
    def get(self, file_path: str) -> TsFileReader:
        if file_path in self._cache:
            return self._cache[file_path]
        else:
            reader = TsFileReader(file_path)
            if (len(self._cache) >= self.max_size):
                oldest_key = next(iter(self._cache))
                self._cache[oldest_key].close()
                del self._cache[oldest_key]
            self._cache[file_path] = reader
            return reader
    def clear(self):
        for reader in self._cache.values():
            reader.close()
        self._cache.clear()