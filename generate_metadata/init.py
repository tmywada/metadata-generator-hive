from libs import HandleValues
from libs import HandleBitmap
from libs import load_config
from libs import generate_chunks
from itertools import tee


class Metadata:
    def __init__(self):
        config = load_config()
        self.chunk_size = config['chunk_size']
        self.handle_values = HandleValues(config)
        self.handle_bitmap = HandleBitmap(config)        

    def reset(self):
        config = load_config()
        self.handle_values = HandleValues(config)
        self.handle_bitmap = HandleBitmap(config)

    def update_chunk_size(self, chunk_size: int):
        self.chunk_size = chunk_size

    def process(self, iterable):
        for chunk in generate_chunks(iterable, self.chunk_size):
            chunk, _chunk = tee(chunk)
            self.hb.update_bitmap(chunk)
            self.hv.separage_values(_chunk)
        return (self.hb.bitmap, self.hv.generate_metadata_values())

    def save_bitmap(self, file_path: str = None):
        """ """
        self.handle_bitmap.save_bitmap(file_path)

    def load_bitmap(self, file_path: str):
        """ """
        self.handle_bitmap.load_bitmap(file_path)

    def save_metadat(self, file_path: str = None):
        """ """
        self.handle_values.save_metadata(file_path)

    def load_metadata(self, file_path: str):
        """ """
        self.handle_values.load_metadata(file_path)
