import bz2
import mmh3
from pyroaring import BitMap


class HandleBitmap:
    def __init__(self, config: dict):
        self.seed = config["seed"]
        self.file_path = config["file_path_bitmap"]
        self.bitmap = BitMap()

    def generate_hash_value(self, string: str, seed: int):
        """ """
        return mmh3.hash(string, seed=seed, signed=False)

    def genearte_hash_values(self, iterable):
        """ """
        return (self.generate_hash_value(
            string=v, seed=self.seed
        ) for v in iterable)

    def update_bitmap(self, iterable):
        """ """
        return self.bitmap.update(self.genearte_hash_values(iterable))

    def save_bitmap(self, file_path: str = None):
        """ """
        if file_path is None:
            file_path = self.file_path
        with bz2.BZ2File(file_path, 'wb') as f:
            f.write(self.bitmap.serialize())

    def load_bitmap(self, file_path: str):
        """ """
        with bz2.BZ2File(file_path, 'rb') as f:
            return BitMap.deserialize(f.read())
