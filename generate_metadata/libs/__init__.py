from libs.handle_bitmap import HandleBitmap
from libs.handle_values import HandleValues

import json
from itertools import chain
from itertools import islice

__all__ = ["HandleBitmap", "HandleValues"]


def load_config():
    file_path_config = './config/config.json'
    return json.load(open(file_path_config, "r"))


def generate_chunks(iterable, size):
    """
    ref: https://stackoverflow.com/a/24527424
    """
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size-1))

