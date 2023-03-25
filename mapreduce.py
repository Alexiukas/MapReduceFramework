import pandas as pd
import os
from collections import defaultdict


class MapReduce:
    def __init__(self, mapper, reducer, output_dataset):
        self.mapper = mapper
        self.reducer = reducer
        self.output_dataset = output_dataset
        self.intermediate = defaultdict(list)

        self.map_reduce()

    def map_reduce(self):
        for directory, map_fn in self.mapper.items():
            data = pd.concat([pd.read_csv(directory + file) for file in os.listdir(directory)])
            for row in data.iterrows():
                map_fn(row, self.intermediate)

        result = []

        for key, values in self.intermediate.items():
            reduced = self.reducer(key, values)

            if reduced is not None:
                if len(self.mapper) == 2:
                    for value in reduced:
                        result.append(value)
                else:
                    result.append(reduced)

        pd.DataFrame(result).to_csv(self.output_dataset)
