import dask.dataframe as dd
import json


class ColumnsRemover:

    def __init__(self, input_path, output_path, rules_path):
        self.input_path = input_path
        self.output_path = output_path
        self.rules_path = rules_path
        self.init_column_list()

    def process(self):
        for column in self.columns_to_remove:
            self.df = self.df.drop(column, axis=1)

    def write_to_file(self):
        self.df.to_csv(self.output_path, single_file=True, index=False)

    def init_column_list(self):
        self.columns_to_remove = []

        with open(self.rules_path, 'r') as f:
            for r in json.load(f)['rules']:
                if r['action'] == 'remove':
                    self.columns_to_remove.append(r['column_name'])

    def read_csv(self):
        self.df = dd.read_csv(self.input_path)
