The scheduler module
====================

.. automodule:: sduds.lib.scheduler

.. autoclass:: IntervalPattern
    :members: __init__, next_clearance

.. autoclass:: CronPattern
    :members: __init__, next_clearance

.. autoclass:: Job
    :members: __init__, overdue, terminate

Lower-level classes and functions
---------------------------------

.. autoclass:: TimePattern
    :members: next_clearance

.. autofunction:: parse_field

.. autofunction:: normalize

.. autoclass:: NoMatchingTimestamp
