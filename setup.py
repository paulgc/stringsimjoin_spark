from setuptools import setup

# Set this to True to enable building extensions using Cython.
# Set it to False to build extensions from the C file (that
# was previously created using Cython).
# Set it to 'auto' to build with Cython if available, otherwise
# from the C file.



import sys



setup(
        name='stringsimjoin_spark',
        version='0.1.0',
        description='Python library for string join in Spark.',
        long_description="""
    String matching is an important problem in many settings such as data integration, natural language processing,etc.
    This package aims to implement most commonly used string matching measures.
    """,
        url='http://github.com/anhaidgroup/stringsimjoin_spark',
        author='Pradap Konda',
        author_email='pradap@cs.wisc.edu',
        license=['MIT'],
        packages=['stringsimjoin_spark', 'stringsimjoin_spark.filter', 'stringsimjoin_spark.index', 'stringsimjoin_spark.join', 'stringsimjoin_spark.utils'],
        install_requires=[],
        include_package_data=True,
        zip_safe=False
)
