"""
Before running, make sure the proxy is running and that the shared go library has been built via
the command: `go build -o ecClient.so -buildmode=c-shared go_client.go`
"""

import argparse
from ctypes import cdll, c_void_p, c_char_p, string_at, CDLL


# # Run: `go build -o ecClient.so -buildmode=c-shared go_client.go`
def load_go_lib(library_path: str) -> CDLL:
    """Load the Go library that was exported to a .so file."""
    return cdll.LoadLibrary(library_path)


def run_go_funcs(go_library: CDLL, client_input: str):
    # Need to make sure to free any pointers
    go_library.free.argtypes = [c_void_p]

    go_library.getAndPut.argtypes = [c_char_p]
    go_library.getAndPut.restype = c_void_p

    clientResultPtr = go_library.getAndPut(client_input.encode('utf-8'))
    clientResultStr = string_at(clientResultPtr)
    go_library.free(clientResultPtr)

    print(clientResultStr.decode('utf-8'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Python client for InfiniCache")
    parser.add_argument("--go_lib_path", help="Path to the built go library", default="./ecClient.so")
    parser.add_argument("--client_input", help="String input to the cache (as a test for now)", default="Cache Test")
    args = parser.parse_args()

    go_lib = load_go_lib(args.go_lib_path)
    run_go_funcs(go_lib, args.client_input)
