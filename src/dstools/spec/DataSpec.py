from copy import deepcopy

import yaml
import pandas as pd
import numpy as np


class DataSpec:
    def __init__(self):
        self.df = None
        self.unique = None
        self.types = None
        self.nas_prop = None
        self.spec = None

    @classmethod
    def from_df(cls, df):
        """Init a data spec by extracting rules from a pandas.DataFrame
        """
        obj = cls()
        obj.df = df
        obj.unique = obj._unique()
        obj.types = obj._types()
        obj.nas_prop = obj._nas_prop()
        obj.spec = obj._spec()
        return obj

    @classmethod
    def from_dict(cls, d):
        """Init a data spec from a dictionary
        """
        obj = cls()
        obj.df = None
        obj.unique = None
        obj.types = {k: spec['kind'] for k, spec in d.items()}
        obj.nas_prop = {k: spec['nas_prop'] for k, spec in d.items()}
        obj.spec = deepcopy(d)
        return obj

    @classmethod
    def from_yaml(cls, path):
        """Init a data spec from a YAML file
        """
        with open(path) as f:
            d = yaml.safe_load(f)

        return cls.from_dict(d)

    def _infer_type(self, col, arr):
        if arr.dtype.kind == 'f':
            return 'numeric'
        # TODO: calling .unique() on every column might be slow, compute it
        # lazily, for finding id columns maybe .is_unique() is faster
        if arr.dtype.kind != 'f' and len(arr) == self.unique[col]:
            return 'id'
        elif self.unique[col] <= 10:
            return 'categorical'
        else:
            return 'numeric'

    def _unique(self):
        return self.df.apply(lambda arr: len(arr.unique()), axis=0)

    def _types(self):
        types = {col: self._infer_type(col, self.df[col]) for col in self.df}
        return types

    def _nas_prop(self):
        return {col: float(self.df[col].isna().mean()) for col in self.df}

    def _spec_numeric(self, arr):
        d = {}
        d['range'] = [float(arr.min()), float(arr.max())]
        return d

    def _spec_categorical(self, arr):
        d = {}
        values = arr.unique().tolist()

        if np.nan in values:
            values.remove(np.nan)

        d['values'] = values
        return d

    def _spec_id(self, arr):
        return {}

    def _validate_numeric(self, col, arr):
        min_, max_ = self.spec[col]['range']
        return (min_ <= arr) & (arr <= max_)

    def _validate_categorical(self, col, arr):
        values = self.spec[col]['values']
        return arr.isin(values)

    def _validate_id(self, col, arr):
        # NOTE: col is not used, but added as arg for consistency
        counts = arr.value_counts()
        duplicates = counts[counts > 1].index
        return ~arr.isin(duplicates)

    def _spec(self):
        d = {}

        for col in self.df:
            arr = self.df[col]
            kind = self.types[col]
            d[col] = {'kind': kind, 'nas_prop': self.nas_prop[col]}

            # add type specific keys
            fn = getattr(self, '_spec_' + kind)
            d[col] = {**d[col], **fn(arr)}

        return d

    def to_dict(self):
        return deepcopy(self.spec)

    def to_yaml(self, path):
        d = self.to_dict()

        with open(path, 'w') as f:
            yaml.dump(d, f)

    def validate(self, df, collapse=True):
        is_valid = {}

        # NOTE: what if we generate another spec from the sample and compare
        # it to the original one, sounds like a rows might be valid alone
        # (e.g. if it has nas and its column has na_proportion > 0) but the
        # column might not be valid overall (if na_proportion_sample >
        # na_proportion_from_spec)

        for col in df:
            arr = df[col]
            kind = self.types[col]
            fn = getattr(self, '_validate_' + kind)
            is_valid_arr = fn(col, arr)

            # NOTE: for now, say an obsevation is valid if it has an NA and
            # the columns na_prop is > 0
            nas_prop = self.nas_prop[col]
            is_valid_na = arr.isna() & (nas_prop > 0)

            is_valid[col] = is_valid_na | is_valid_arr

        is_valid_df = pd.DataFrame(is_valid)
        is_valid_df.columns = df.columns

        if collapse:
            # return False if there is at least one invalid observation
            return not (~is_valid_df).sum().sum() > 0
        else:
            return is_valid_df


def to_df(data):
    df = pd.DataFrame(data['data'])
    df.columns = data['feature_names']
    df['target'] = data['target']
    return df
