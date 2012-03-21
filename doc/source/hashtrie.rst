hash trie module
================

.. automodule:: sduds.hashtrie

.. autoclass:: HashTrie
    :members: __init__, add, delete, contains, get_missing_hashes_as_server, get_missing_hashes_as_client, close

Usage
-----

The following example creates two :class:`HashTrie` instances, adds a hash to one of them and shows how to use
the synchronization methods to get this hash from the other instance:

.. literalinclude:: hashtrie_example.py
