"""Distributed MapReduce framework backend package.

A from-scratch Master/Worker MapReduce engine: HTTP-based node communication,
JSON-file storage keyed by job and stage, fault tolerance with retries, and a
Shuffle layer that streams partitions from mappers to reducers over HTTP.
"""

__version__ = "1.0.0"
