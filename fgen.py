import polars as pl
from datetime import datetime
from typing import List, Dict, Any, Union
from dataclasses import dataclass, field
from collections import OrderedDict
import warnings


def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


# @singleton
@dataclass
class Filter:
    """..."""
    source: str
    column: str
    operator: str
    value: Any
    name: str
    mask: pl.Series = None
    order_key: int = 0
    
    def calc(self, df: pl.DataFrame) -> pl.Series:
        if self.operator == 'eq':
            mask = df[self.column].eq(self.value)
        elif self.operator == 'eq_missing':
            mask = df[self.column].eq_missing(self.value)
        elif self.operator == 'ne':
            mask = df[self.column].ne(self.value)
        elif self.operator == 'ne_missing':
            mask = df[self.column].ne_missing(self.value)
        elif self.operator == 'ge':
            mask = df[self.column].ge(self.value)
        elif self.operator == 'le':
            mask = df[self.column].le(self.value)
        elif self.operator == 'gt':
            mask = df[self.column].gt(self.value)
        elif self.operator == 'lt':
            mask = df[self.column].lt(self.value)
        elif self.operator == 'is_in':
            mask = df[self.column].is_in(self.value)
        elif self.operator == 'is_nan':
            mask = df[self.column].is_nan()
        elif self.operator == 'is_not_nan':
            mask = df[self.column].is_not_nan()
        elif self.operator == 'is_between':
            mask = df[self.column].is_between(**self.value)
        else:
            raise NotImplementedError(f'Оператор {self.operator} не поддерживается')
        
        return mask
    
    def fit(self, X: pl.DataFrame, y: pl.Series = None):
        self.mask = self.calc(X)
        
        return self
    
    def transform(self, X: pl.DataFrame, y: pl.Series = None) -> pl.DataFrame:
        if self.is_fitted:
            output = X.filter(self.mask)
        else:
            mask = self.calc(X) 
            output = X.filter(mask)
        
        return output
    
    def purge(self):
        self.mask = None
        
        return self
    
    @property
    def is_fitted(self):
        return self.mask is not None


# @singleton
@dataclass
class FilterSet:
    """Класс, содержащий цепочку фильтров, отсортированных в лексикографическом порядке.
    
    arguments:
        source: Источник данных
        filters: Список фильтров
        name: Название цепочки фильтров. Если не указывать, по умолчанию название формируется путем склейки названий фильтров через '_'.
            `name` используется для сортировки цепочек фильтров в `FilteredChain`
    """
    source: str
    filters: Union[Filter, List[Filter], OrderedDict[str, Filter], Dict[str, Filter]]
    name: str = None
    
    @property
    def filters(self):
        return self._filters
    
    @filters.setter
    def filters(self, filters: Union[Filter, List[Filter], OrderedDict[str, Filter], Dict[str, Filter]]):
        if isinstance(filters, list):
            self._filters = OrderedDict({f.name: f for f in sorted(filters, key=lambda x: (x.order_key, x.name))})
            self._filters_list = list(self._filters.values())
        elif isinstance(filters, OrderedDict) or isinstance(filters, dict):
            self._filters = OrderedDict({k: filters[k] for k, v in sorted(filters.items(), key=lambda x: (x[1].order_key, x[1].name))})
            self._filters_list = list(self._filters.values())
        elif isinstance(filters, Filter):
            self._filters = OrderedDict({filters.name: filters})
            self._filters_list = list(self._filters.values())
        else:
            raise NotImplementedError(f'Неправильный аргумент `filters`: {filters}')
            # warnings.warn('Принято нерекоммендованное значение для аргумента `filters`. Фильтры не были отсортированы!')
            # self._filters = filters
            # self._filters_list = [f for f in filters] # TBD ...
            
    @property
    def filters_list(self):
        return self._filters_list
        
    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, name: str):
        self._name = '_'.join([v.name for k, v in self.filters.items()]) if name is None or isinstance(name, property) else name
        
    def __getitem__(self, value):
        try:
            if isinstance(value, int) or isinstance(value, slice):
                return self.filters_list[value]
            elif isinstance(value, str):
                return self.filters[value]
            else:
                raise IndexError(f'incorrect index value: {value}')
                
        except IndexError as e:
            keys = []
            for i, k in enumerate(self.keys()):
                if i == 10: break
                else: keys.append(k)
            
            e.args += (
                f'provided value: {value}', 
                f'length of filters: {len(self)}', 
                f'top 10 available keys: {keys}',
            )
            raise e
            
    def __len__(self):
        return len(self.filters)
    
    def values(self):
        return self.filters.values()
    
    def keys(self):
        return self.filters.keys()
    
    def items(self):
        return self.filters.items()
    
    def __iter__(self):
        for v in self._filters_list:
            yield v
    
    def fit(self, X: pl.DataFrame, y: pl.Series = None):
        for f in self:
            if f.is_fitted: continue
            else: f.fit(X)
        
        return self
    
    def transform(self, X: pl.DataFrame, y: pl.Series = None) -> pl.DataFrame:
        if self.is_empty:
            return X
        elif self.is_fitted:
            output = X.filter(self.mask)
        else:
            mask_list = []
            for f in self:
                mask = f.calc(X) if not f.is_fitted else f.mask
                mask_list.append(mask)
                
            mask = self.merge_mask_list(mask_list)
            output = X.filter(mask)
        
        return output
    
    def purge_all(self):
        for f in self:
            if f.is_fitted: f.purge()
        
        return self
    
    @property
    def is_fitted(self):
        if self.is_empty: return None
        else: return all(f.is_fitted for f in self)
    
    @property
    def mask_list(self):
        return [f.mask for f in self]
    
    @property
    def mask(self):
        if self.is_fitted:
            return self.merge_mask_list(self.mask_list)
        else:
            return None
        
    @staticmethod
    def merge_mask_list(mask_list):
        if len(mask_list) == 0: return None
        if len(mask_list) == 1: return mask_list[0]
        mask = mask_list[0]
        for m in mask_list[1:]:
            mask &= m # TBD
        return mask
    
    @property
    def is_empty(self) -> bool:
        return len(self) == 0


# @singleton
@dataclass
class FilteredChain:
    """..."""
    filter_sets: List[FilterSet]
    do_fit: bool = True
    n_cache_filters: int = 50
    fitted_filters: OrderedDict[str, Filter] = field(default_factory = lambda: OrderedDict())
    empty_filter_nodes: List[str] = field(default_factory = lambda: list())
    
    @property
    def filter_sets(self):
        return self._filter_sets
    
    @property
    def filter_sets_list(self):
        return self._filter_sets_list
    
    @property
    def filter_sets_tree(self):
        return self._filter_sets_tree
    
    @filter_sets.setter
    def filter_sets(self, filter_sets: List[FilterSet]):
        if isinstance(filter_sets, list):
            self._filter_sets = OrderedDict({f.name: f for f in sorted(filter_sets, key=lambda x: x.name)})
            self._filter_sets_list = list(self._filter_sets.values())
            self._filter_sets_tree = self.build_filter_tree(self._filter_sets_list)
        elif isinstance(filter_sets, FilterSet):
            self._filter_sets = OrderedDict({filter_sets.name: filter_sets})
            self._filter_sets_list = list(self._filter_sets.values())
            self._filter_sets_tree = self.build_filter_tree(self._filter_sets_list)
        else:
            raise NotImplementedError(f'Неправильный аргумент `filter_sets`: {filter_sets}')
    
    def fit_filter_set(self, filter_set: FilterSet, data: pl.DataFrame):
        if self.do_fit:
            filter_set.fit(data)

            for f in filter_set:
                self.fitted_filters[f.name] = f
        
        return self
    
    def purge_n_fitted_filters(self, n):
        for i, fitted_filter in enumerate(self.fitted_filters.copy().values()):
            self.fitted_filters[fitted_filter.name].purge()
            del self.fitted_filters[fitted_filter.name]
            if i == n - 1:
                break
    
    def apply(self, data: pl.DataFrame):
        for filter_set in self.filter_sets_list:
            self.fit_filter_set(filter_set, data)
            
            if len(self.fitted_filters) > self.n_cache_filters >= 0:
                n_filters_to_purge = len(self.fitted_filters) - self.n_cache_filters
                self.purge_n_fitted_filters(n_filters_to_purge)
            
            output = filter_set.transform(data)
            
            yield filter_set, output
            
    def purge_all(self):
        for f in self.filter_sets_list:
            f.purge_all()
        
        self.fitted_filters = OrderedDict()
        self.empty_filter_nodes = list()
        
        return self
    
    @staticmethod
    def build_filter_tree(filter_sets_list):
        
        def insert_into_tree(tree, filter_set):
            current_level = tree
            node_name = ''
            for f in filter_set:
                node_name += f'_{f.name}' if len(node_name) > 0 else f.name # TBD
                if node_name not in current_level:
                    current_level[node_name] = {}
                current_level = current_level[node_name]
            current_level['FilterSet'] = filter_set
        
        filter_tree = {}
        for filter_set in filter_sets_list:
            insert_into_tree(filter_tree, filter_set)
                
        return filter_tree
    
    def apply_filter_tree(self, data: pl.DataFrame):
        yield from self._iterate_filter_tree(self.filter_sets_tree.copy(), data)
    
    def _iterate_filter_tree(self, filter_tree, data: pl.DataFrame):
        for k, v in filter_tree.items():
            if k == 'FilterSet':
                self.fit_filter_set(v, data)
            
                if len(self.fitted_filters) > self.n_cache_filters >= 0:
                    n_filters_to_purge = len(self.fitted_filters) - self.n_cache_filters
                    self.purge_n_fitted_filters(n_filters_to_purge)
                
                if v.mask.sum() == 0:
                    self.empty_filter_nodes.append(v.name)
                    continue
                
                output = v.transform(data)

                yield v, output
            else:
                for f in self.empty_filter_nodes:
                    if f in k:
                        self.empty_filter_nodes.append(k)
                        break
                else:
                    yield from self._iterate_filter_tree(v, data)






@dataclass
class Feature:
    """Class for storing configs of features."""
    source: str
    id_column: str
    column: str
    dtype: Union[str, type]
    aggregation: Union[str, Any] = 'mean'
    filter_set: Union[FilterSet, Filter, List[Dict[str, Any]], List[Filter]] = None
    version: int = None
    name: str = None
    description: str = None
    # filter_conjunction: str = None
    # filter_name: str = None
    logs: List[Dict] = field(default_factory = lambda: list())
    
    @property
    def filter_set(self):
        return self._filter_set
    
    @filter_set.setter
    def filter_set(self, filter_set: Union[FilterSet, Filter, List[Dict[str, Any]], List[Filter]]):
        if isinstance(filter_set, FilterSet):
            self._filter_set = filter_set
        elif isinstance(filter_set, Filter):
            self._filter_set = FilterSet(source=self.source, filters=[filter_set])
        elif isinstance(filter_set, list):
            if len(filter_set) == 0:
                self._filter_set = FilterSet(source=self.source, filters=[])
            elif isinstance(filter_set[0], Filter): # and other are also filters... 
                self._filter_set = FilterSet(source=self.source, filters=filter_set)
            elif isinstance(filter_set[0], dict):   # and other are also dict... 
                self._filter_set = FilterSet(source=self.source, filters=[Filter(**f) for f in filter_set])
        else:
            raise NotImplementedError(f'Неправильный аргумент `filter_set`: {filter_set}')
          
    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, name: str):
        if name is None or isinstance(name, property):
            version = f'_v{str(self.version)}' if self.version is not None else ''
            self._name = f'{self.aggregation}_{self.column}_{self.filter_set.name}{version}'
        else:
            self._name = name
    
@dataclass
class FeatureSet:
    """Class for merging set of features based on filters and aggregations."""
    source: str
    id_column: str
    features: Union[Feature, List[Feature], OrderedDict[str, Feature], Dict[str, Feature]]
    
    @property
    def features(self):
        return self._features
    
    @features.setter
    def features(self, features: Union[Feature, List[Feature], OrderedDict[str, Feature], Dict[str, Feature]]):
        if isinstance(features, list):
            self._features = OrderedDict({f.name: f for f in sorted(features, key=lambda x: x.name)})
            self._features_list = list(self._features.values())
        elif isinstance(features, OrderedDict) or isinstance(filters, dict):
            self._features = OrderedDict({k: features[k] for k, v in sorted(features.items(), key=lambda x: x[1].name)})
            self._features_list = list(self._features.values())
        elif isinstance(features, Feature):
            self._features = OrderedDict({features.name: features})
            self._features_list = list(self._features.values())
        else:
            raise NotImplementedError(f'Неправильный аргумент `filters`: {filters}')




data = {
    'REQUESTID': [1, 1, 2, 2, 3, 3],
    'CREDITSUM': [1000, 2000, 1500, 3000, 1000, 500],
    'IS_OWN': [1, 1, 0, 1, 0, 0],
    'LOAN_TYPE': [7, 9, 10, 9, 9, 10]
}

df = pl.DataFrame(data)


f_0 = Filter(**{
    'name': '',
    'column': 'REQUESTID',
    'operator': 'is_not_nan',
    'value': -1,
    'source': 'cr_loan',
    'order_key': 0, 
})
f_1 = Filter(**{
    'name': 'MTSB',
    'column': 'IS_OWN',
    'operator': 'eq',
    'value': 1,
    'source': 'cr_loan',
    'order_key': 0, 
})
f_2 = Filter(**{
    'name': 'POTREB',
    'column': 'LOAN_TYPE',
    'operator': 'is_in',
    'value': [9],
    'source': 'cr_loan',
    'order_key': 1, 
})
f_3 = Filter(**{
    'name': 'MICRO',
    'column': 'LOAN_TYPE',
    'operator': 'is_in',
    'value': [21],
    'source': 'cr_loan',
    'order_key': 1, 
})
f_4 = Filter(**{
    'name': 'CREDITSUM_GE_2000',
    'column': 'CREDITSUM',
    'operator': 'ge',
    'value': 2000,
    'source': 'cr_loan',
    'order_key': 2, 
})

f_s_0 = FilterSet(source='cr_loan', filters=[f_0])
f_s_1 = FilterSet(source='cr_loan', filters=[f_2, f_1])
f_s_2 = FilterSet(source='cr_loan', filters=[f_1])
f_s_3 = FilterSet(source='cr_loan', filters=[f_2])
f_s_4 = FilterSet(source='cr_loan', filters=[f_1, f_2])
f_s_5 = FilterSet(source='cr_loan', filters=[f_3, f_1])
f_s_6 = FilterSet(source='cr_loan', filters=[f_3])
f_s_7 = FilterSet(source='cr_loan', filters=[f_3, f_1, f_4])

filtered_chain = FilteredChain([f_s_0, f_s_1, f_s_2, f_s_3, f_s_4, f_s_5, f_s_6, f_s_7], do_fit=True, n_cache_filters=-1)
