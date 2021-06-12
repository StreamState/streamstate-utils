[![codecov](https://codecov.io/gh/StreamState/streamstate-utils/branch/master/graph/badge.svg?token=IwdDtmI5kN)](https://codecov.io/gh/StreamState/streamstate-utils)

# streamstate-utils

Set of utils for managing cassandra and pyspark.  Intended for use only with StreamState.

# To test with spark

PYSPARK_PYTHON=python3 spark-submit --packages org.streamstate:streamstate-utils_2.12:0.1.0-SNAPSHOT setup.py test