#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This Protocol reuses core Pandas annotation.
# Overall pipeline looks as follows
# - Stubgen pandas.core.series
# - Add Protocol as a base class
# - Replace imports with Any

import numpy as np
from typing import Any, Callable, Hashable, IO, Optional
from typing_extensions import Protocol

groupby_generic = Any

class SeriesLike(Protocol):
    hasnans: Any = ...
    div: Callable[[SeriesLike, Any], SeriesLike]
    rdiv: Callable[[SeriesLike, Any], SeriesLike]
    def __init__(
        self,
        data: Optional[Any] = ...,
        index: Optional[Any] = ...,
        dtype: Optional[Any] = ...,
        name: Optional[Any] = ...,
        copy: bool = ...,
        fastpath: bool = ...,
    ) -> None: ...
    @property
    def dtype(self): ...
    @property
    def dtypes(self): ...
    @property
    def name(self) -> Optional[Hashable]: ...
    @name.setter
    def name(self, value: Optional[Hashable]) -> None: ...
    @property
    def values(self): ...
    def ravel(self, order: str = ...): ...
    def __len__(self) -> int: ...
    def view(self, dtype: Optional[Any] = ...): ...
    def __array_ufunc__(self, ufunc: Callable, method: str, *inputs: Any, **kwargs: Any) -> Any: ...
    def __array__(self, dtype: Any = ...) -> np.ndarray: ...
    __float__: Any = ...
    __long__: Any = ...
    __int__: Any = ...
    @property
    def axes(self): ...
    def take(self, indices: Any, axis: int = ..., is_copy: bool = ..., **kwargs: Any): ...
    def __getitem__(self, key: Any): ...
    def __setitem__(self, key: Any, value: Any) -> None: ...
    def repeat(self, repeats: Any, axis: Optional[Any] = ...): ...
    index: Any = ...
    def reset_index(
        self,
        level: Optional[Any] = ...,
        drop: bool = ...,
        name: Optional[Any] = ...,
        inplace: bool = ...,
    ): ...
    def to_string(
        self,
        buf: Optional[Any] = ...,
        na_rep: str = ...,
        float_format: Optional[Any] = ...,
        header: bool = ...,
        index: bool = ...,
        length: bool = ...,
        dtype: bool = ...,
        name: bool = ...,
        max_rows: Optional[Any] = ...,
        min_rows: Optional[Any] = ...,
    ): ...
    def to_markdown(
        self, buf: Optional[IO[str]] = ..., mode: Optional[str] = ..., **kwargs: Any
    ) -> Optional[str]: ...
    def items(self): ...
    def iteritems(self): ...
    def keys(self): ...
    def to_dict(self, into: Any = ...): ...
    def to_frame(self, name: Optional[Any] = ...): ...
    def groupby(
        self,
        by: Any = ...,
        axis: Any = ...,
        level: Any = ...,
        as_index: bool = ...,
        sort: bool = ...,
        group_keys: bool = ...,
        squeeze: bool = ...,
        observed: bool = ...,
    ) -> Any: ...
    def count(self, level: Optional[Any] = ...): ...
    def mode(self, dropna: bool = ...): ...
    def unique(self): ...
    def drop_duplicates(self, keep: str = ..., inplace: bool = ...): ...
    def duplicated(self, keep: str = ...): ...
    def idxmin(self, axis: int = ..., skipna: bool = ..., *args: Any, **kwargs: Any): ...
    def idxmax(self, axis: int = ..., skipna: bool = ..., *args: Any, **kwargs: Any): ...
    def round(self, decimals: int = ..., *args: Any, **kwargs: Any): ...
    def quantile(self, q: float = ..., interpolation: str = ...): ...
    def corr(self, other: Any, method: str = ..., min_periods: Optional[Any] = ...): ...
    def cov(self, other: Any, min_periods: Optional[Any] = ...): ...
    def diff(self, periods: int = ...): ...
    def autocorr(self, lag: int = ...): ...
    def dot(self, other: Any): ...
    def __matmul__(self, other: Any): ...
    def __rmatmul__(self, other: Any): ...
    def searchsorted(self, value: Any, side: str = ..., sorter: Optional[Any] = ...): ...
    def append(self, to_append: Any, ignore_index: bool = ..., verify_integrity: bool = ...): ...
    def combine(self, other: Any, func: Any, fill_value: Optional[Any] = ...): ...
    def combine_first(self, other: Any): ...
    def update(self, other: Any) -> None: ...
    def sort_values(
        self,
        axis: int = ...,
        ascending: bool = ...,
        inplace: bool = ...,
        kind: str = ...,
        na_position: str = ...,
        ignore_index: bool = ...,
    ): ...
    def sort_index(
        self,
        axis: Any = ...,
        level: Any = ...,
        ascending: Any = ...,
        inplace: Any = ...,
        kind: Any = ...,
        na_position: Any = ...,
        sort_remaining: Any = ...,
        ignore_index: bool = ...,
    ) -> Any: ...
    def argsort(self, axis: int = ..., kind: str = ..., order: Optional[Any] = ...): ...
    def nlargest(self, n: int = ..., keep: str = ...): ...
    def nsmallest(self, n: int = ..., keep: str = ...): ...
    def swaplevel(self, i: int = ..., j: int = ..., copy: bool = ...): ...
    def reorder_levels(self, order: Any): ...
    def explode(self) -> SeriesLike: ...
    def unstack(self, level: int = ..., fill_value: Optional[Any] = ...): ...
    def map(self, arg: Any, na_action: Optional[Any] = ...): ...
    def aggregate(self, func: Any, axis: int = ..., *args: Any, **kwargs: Any): ...
    agg: Any = ...
    def transform(self, func: Any, axis: int = ..., *args: Any, **kwargs: Any): ...
    def apply(self, func: Any, convert_dtype: bool = ..., args: Any = ..., **kwds: Any): ...
    def align(
        self,
        other: Any,
        join: str = ...,
        axis: Optional[Any] = ...,
        level: Optional[Any] = ...,
        copy: bool = ...,
        fill_value: Optional[Any] = ...,
        method: Optional[Any] = ...,
        limit: Optional[Any] = ...,
        fill_axis: int = ...,
        broadcast_axis: Optional[Any] = ...,
    ): ...
    def rename(
        self,
        index: Optional[Any] = ...,
        *,
        axis: Optional[Any] = ...,
        copy: bool = ...,
        inplace: bool = ...,
        level: Optional[Any] = ...,
        errors: str = ...,
    ): ...
    def reindex(self, index: Optional[Any] = ..., **kwargs: Any): ...
    def drop(
        self,
        labels: Optional[Any] = ...,
        axis: int = ...,
        index: Optional[Any] = ...,
        columns: Optional[Any] = ...,
        level: Optional[Any] = ...,
        inplace: bool = ...,
        errors: str = ...,
    ): ...
    def fillna(
        self,
        value: Any = ...,
        method: Any = ...,
        axis: Any = ...,
        inplace: Any = ...,
        limit: Any = ...,
        downcast: Any = ...,
    ) -> Optional[SeriesLike]: ...
    def replace(
        self,
        to_replace: Optional[Any] = ...,
        value: Optional[Any] = ...,
        inplace: bool = ...,
        limit: Optional[Any] = ...,
        regex: bool = ...,
        method: str = ...,
    ): ...
    def shift(
        self,
        periods: int = ...,
        freq: Optional[Any] = ...,
        axis: int = ...,
        fill_value: Optional[Any] = ...,
    ): ...
    def memory_usage(self, index: bool = ..., deep: bool = ...): ...
    def isin(self, values: Any): ...
    def between(self, left: Any, right: Any, inclusive: bool = ...): ...
    def isna(self): ...
    def isnull(self): ...
    def notna(self): ...
    def notnull(self): ...
    def dropna(self, axis: int = ..., inplace: bool = ..., how: Optional[Any] = ...): ...
    def to_timestamp(self, freq: Optional[Any] = ..., how: str = ..., copy: bool = ...): ...
    def to_period(self, freq: Optional[Any] = ..., copy: bool = ...): ...
    str: Any = ...
    dt: Any = ...
    cat: Any = ...
    plot: Any = ...
    sparse: Any = ...
    hist: Any = ...
