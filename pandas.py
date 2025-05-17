import csv

class Series(list):
    def apply(self, func):
        return Series([func(x) for x in self])
    def sum(self):
        return sum(self)
    def __le__(self, other):
        return [x <= other for x in self]
    def __gt__(self, other):
        return [x > other for x in self]

class _Values(list):
    @property
    def shape(self):
        rows = len(self)
        cols = len(self[0]) if rows else 0
        return (rows, cols)
    def tolist(self):
        return list(self)

class _ILocIndexer:
    def __init__(self, df):
        self.df = df
    def __getitem__(self, key):
        if isinstance(key, int):
            return self.df.data[key]
        row_idx, col_idx = key
        row = self.df.data[row_idx]
        if isinstance(col_idx, int):
            return row[col_idx]
        return [row[i] for i in col_idx]

class _LocIndexer:
    def __init__(self, df):
        self.df = df
    def __getitem__(self, key):
        mask, _ = key
        new_data = [row for row, flag in zip(self.df.data, mask) if flag]
        return DataFrame(new_data)

class DataFrame:
    def __init__(self, data):
        self.data = [list(row) for row in data]
    def sort_values(self, by=0, ascending=True, inplace=False):
        sorted_data = sorted(self.data, key=lambda row: row[by], reverse=not ascending)
        if inplace:
            self.data = sorted_data
        else:
            return DataFrame(sorted_data)
    def drop(self, index=None, columns=None, inplace=False):
        new_data = self.data
        if index is not None:
            if not isinstance(index, (list, set, tuple)):
                index = [index]
            new_data = [row for i, row in enumerate(new_data) if i not in index]
        if columns is not None:
            if not isinstance(columns, (list, set, tuple)):
                columns = [columns]
            cols = sorted(columns, reverse=True)
            new_data = [row[:]+[] for row in new_data]
            for row in new_data:
                for c in cols:
                    del row[c]
        if inplace:
            self.data = new_data
        else:
            return DataFrame(new_data)
    @property
    def values(self):
        return _Values(self.data)
    @property
    def iloc(self):
        return _ILocIndexer(self)
    @property
    def loc(self):
        return _LocIndexer(self)
    @property
    def index(self):
        return list(range(len(self.data)))
    def __getitem__(self, key):
        return Series([row[key] for row in self.data])
    def __setitem__(self, key, series):
        for i, val in enumerate(series):
            self.data[i][key] = val
    def to_csv(self, filename=None, header=None, index=None, sep=','):
        def generate_csv():
            lines = []
            if header is not None:
                lines.append(sep.join(header))
            for row in self.data:
                lines.append(sep.join(str(x) for x in row))
            return "\n".join(lines) + "\n"

        csv_content = generate_csv()
        if filename is None:
            return csv_content
        with open(filename, 'w') as f:
            f.write(csv_content)
    def __iter__(self):
        return iter(range(len(self.data)))

    def __len__(self):
        return len(self.data)


def _convert(v):
    try:
        if v.strip() == '':
            return v
        if '.' in v or 'e' in v or 'E' in v:
            return float(v)
        return int(v)
    except ValueError:
        return v

def read_csv(filename, header=None, skiprows=0, delimiter=','):
    with open(filename, newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        rows = list(reader)
    rows = rows[skiprows:]
    data = [[_convert(v) for v in row] for row in rows]
    return DataFrame(data)
