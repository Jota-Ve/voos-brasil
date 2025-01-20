import numpy as np


def read_numpy(path: str, encoding: str, max_rows: int|None=None):
    return np.loadtxt(path, encoding=encoding, delimiter=',', dtype=str, max_rows=max_rows)


def read_csv(path: str, encoding: str, max_rows: int|None = None):
    with open(path, encoding=encoding) as f:
        if max_rows:
            return [line.split(',') for line, _ in zip(f, range(max_rows))]

        return [line.split(',') for line in f]
