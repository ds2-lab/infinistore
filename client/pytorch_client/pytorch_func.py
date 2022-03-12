"""
Before running, make sure the proxy is running and that the shared go library has been built via
the command: `go build -o ecClient.so -buildmode=c-shared go_client.go`
"""
import argparse
import random
from ctypes import CDLL, c_char_p, c_void_p, cdll, string_at
from typing import TypeVar

import numpy as np

NumpyDtype = TypeVar("NumpyDtype")


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

    clientResultPtr = go_library.getFromCache(cache_key.encode("utf-8"))

    clientResultStr = string_at(clientResultPtr)
    go_library.free(clientResultPtr)
    if clientResultStr.decode("utf-8") == "NOT_IN":
        raise KeyError("Key is not in cache")
    result_arr = convert_bytes_to_np(clientResultStr, arr_dtype).reshape(arr_shape)
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

    np_bytes_str_enc = convert_np_to_bytes(input_arr)
    go_library.setInCache(cache_key.encode("utf-8"), np_bytes_str_enc)


def convert_np_to_bytes(input_arr: np.ndarray) -> bytes:
    """Returns a string encoded representation of the numpy array in bytes. This gets rid of the
    excessive backslashes that mess up encoding/decoding."""
    np_bytes = input_arr.tobytes()
    return str(np_bytes).encode("utf-8")


def convert_bytes_to_np(input_bytes: bytes, data_type: NumpyDtype) -> np.ndarray:
    """Decode so that the excessive backslashes are removed."""
    bytes_np_dec = input_bytes.decode("unicode-escape").encode("ISO-8859-1")[2:-1]
    return np.frombuffer(bytes_np_dec, dtype=data_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Python client for InfiniCache")
    parser.add_argument(
        "--go_lib_path", help="Path to the built go library", default="./ecClient.so"
    )
    args = parser.parse_args()

    go_lib = load_go_lib(args.go_lib_path)
    random_arr = np.random.randn(50, 50)
    print("Original array shape: ", random_arr.shape)
    print("Original array: ", random_arr)

    random_key = "test_" + str(random.randint(0, 50000))
    print("key: ", random_key)
    try:
        cache_arr = get_array_from_cache(go_lib, random_key, random_arr.dtype, random_arr.shape)
    except KeyError:
        print("passing error")

    random_key = "test_" + str(random.randint(0, 50000))
    print("key: ", random_key)
    set_array_in_cache(go_lib, random_key, random_arr)
    cache_arr = get_array_from_cache(go_lib, random_key, random_arr.dtype, random_arr.shape)
    print("Array from cache shape: ", cache_arr.shape)
    print("Array from cache: ", cache_arr)
