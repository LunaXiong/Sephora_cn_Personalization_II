from distutils.core import setup, Extension
from Cython.Build import cythonize
import numpy

setup(ext_modules=cythonize(Extension(
    'item_cooccurance',
    sources=['item_cooccurance.pyx'],
    language='c',
    include_dirs=[numpy.get_include()],
    library_dirs=[],
    libraries=[],
    extra_compile_args=[],
    extra_link_args=[]
)))