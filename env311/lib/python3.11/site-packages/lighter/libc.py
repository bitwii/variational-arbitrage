import ctypes
import ctypes.util

def get_libc() -> ctypes.CDLL:
    libc_name = ctypes.util.find_library("c")
    if not libc_name:
        raise RuntimeError("Could not find C standard library")

    libc = ctypes.CDLL(libc_name)
    libc.free.argtypes = [ctypes.c_void_p]
    libc.free.restype = None

    return libc

_libc = get_libc()

def free(ptr: int) -> None:
    _libc.free(ptr)

