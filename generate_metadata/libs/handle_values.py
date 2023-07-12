from math import sqrt
import pickle


class HandleValues:
    def __init__(self, config: dict):
        self.chunk_size = config['chunk_size']
        self.file_path = config['file_path_metadata']
        self.source = {}
        self.data_types = []
        self.is_numeric = {}
        for item in config["data_types_supported"]:
            data_type = item["name"]
            is_numeric = item["is_numeric"]
            self.source[data_type] = {
                'values': [],
                'lengths': [],
                'values_aggregated': (),
                'lengths_aggregated': ()
            }
            self.is_numeric[data_type] = is_numeric
            self.data_types.append(data_type)

    def infer_data_type(self, value: str):
        """ """
        _len_value = len(value)
        try:
            _v = int(value)
            _data_type = 'integer'
        except Exception:
            try:
                _v = float(value)
                _data_type = 'float'
            except Exception:
                _v = value
                _data_type = 'string'
        return (_v, _data_type, _len_value)

    def append_elements(self, iterable, values: list):
        yield from iterable
        yield from values

    def separate_value(self, value):
        """ """
        _value, _data_type, _length = self.infer_data_type(value)      
        self.source[_data_type]['values'].append(_value)
        self.source[_data_type]['lengths'].append(_length)

    def aggregate_values_separated(self):
        for data_type in self.data_types:
            self.source[data_type]['values_aggregated'] = self.append_elements(
                iterable=self.source[data_type]['values_aggregated'],
                elements=self.source[data_type]['values']
            )
            self.source[data_type]['lengths_aggregated'] = self.append_elements(
                iterable=self.source[data_type]['lengths_aggregated'],
                elements=self.source[data_type]['lengths']
            )
            self.source[data_type]['values'] = []
            self.source[data_type]['lengths'] = []

    def separate_values(self, iterable):
        """ """
        [self.separate_value(value) for value in iterable]

    def calculate_stats_streaming(self, iterable):
        _cnt = 0
        _mean = 0
        _var = 0
        M2 = 0

        for x in iterable:
            _cnt += 1
            delta = x - _mean
            _mean += delta / _cnt
            delta2 = x - _mean
            M2 += delta * delta2
            _var = M2 / _cnt

        _std = sqrt(_var)
        return _mean, _std, _cnt

    def generate_stats_numeric(self, iterable):
        """ """
        v_mean, v_std, cnt = self.calculate_stats_streaming(
            iterable=iterable
        )
        return (v_mean, v_std, cnt)

    def generate_metadata_values(self):
        """ """
        res = []

        for data_type in self.data_types:

            if self.is_numeric[data_type]:
                v_mean, v_std, _ = self.generate_stats_numeric(
                    self.source[data_type]['values']
                )

                l_mean, l_std, cnt = self.generate_stats_numeric(
                    self.source[data_type]['lengths']
                )
            else:
                v_mean = None
                v_std = None

                l_mean, l_std, cnt = self.generate_stats_numeric(
                    self.source[data_type]['lengths']
                )

            res.append(
                {
                    "data_type": data_type,
                    "count": cnt,
                    "value_mean": v_mean,
                    "value_std": v_std,
                    "length_mean": l_mean,
                    "length_std": l_std
                }
            )
        return res

    def save_metadata(self, metadata: dict, file_path: str = None):
        if file_path is None:
            file_path = self.file_path
        pickle.dump(metadata, open(file_path, 'wb'))

    def load_metadata(self, file_path: str):
        return pickle.load(open(file_path, 'rb'))
