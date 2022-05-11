"""
Before running, make sure the proxy is running and that the shared go library has been built via
the command: `go build -o ecClient.so -buildmode=c-shared go_client.go`
"""
from __future__ import annotations

from functools import reduce
from ctypes import CDLL, c_char_p, c_void_p, cdll, string_at
from typing import TypeVar

import numpy as np
import os

NumpyDtype = TypeVar("NumpyDtype")

NEGATIVE_ASCII_VALUE = 45


# # Run: `go build -o ecClient.so -buildmode=c-shared go_client.go`
def load_go_lib(library_path: str) -> CDLL:
    """Load the Go library that was exported to a .so file."""
    return cdll.LoadLibrary(library_path)


def get_array_from_cache(
    go_library: CDLL, cache_key: str, arr_dtype: NumpyDtype, arr_shape: tuple[int]
) -> np.ndarray:
    """
    Example:
        go_library = load_go_lib(args.go_lib_path)
        cache_key = "test_" + str(random.randint(0, 50000))
        arr_dtype = input_arr.dtype
    """
    # Need to make sure to free any pointers
    go_library.free.argtypes = [c_void_p]
    go_library.getFromCache.argtypes = [c_char_p]
    go_library.getFromCache.restype = c_void_p
    arr_length = reduce(lambda x, y: x * y, arr_shape)

    clientResultPtr = go_library.getFromCache(cache_key.encode("utf-8"))
    clientResultStr = string_at(clientResultPtr, arr_length)

    go_library.free(clientResultPtr)
    if clientResultStr[0] == NEGATIVE_ASCII_VALUE:
        raise KeyError("Key is not in cache")
    result_arr = np.frombuffer(clientResultStr, dtype=arr_dtype).reshape(arr_shape)
    return result_arr


def set_array_in_cache(go_library: CDLL, cache_key: str, input_arr: np.ndarray):
    """
    Example:
        go_library = load_go_lib(args.go_lib_path)
        cache_key = "test_" + str(random.randint(0, 50000))
        input_arr = np.random.randn(2, 2)
    """
    # Need to make sure to free any pointers
    go_library.free.argtypes = [c_void_p]
    go_library.setInCache.argtypes = [c_char_p, c_char_p]

    np_bytes = input_arr.tobytes()
    go_library.setInCache(cache_key.encode("utf-8"), np_bytes, len(np_bytes))

GO_LIB = load_go_lib(os.path.join(os.path.dirname(__file__), "ecClient.so"))
