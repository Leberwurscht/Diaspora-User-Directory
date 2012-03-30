partners module
===============

.. automodule:: sduds.partners

.. autoclass:: Partner
   :members: name, accept_password, base_url, control_probability, last_connection, kicked, connection_schedule, provide_username, provide_password,
             __init__, get_synchronization_address, control_sample

.. autoclass:: PartnerDatabase
   :members: __init__, cleanup, get_partners, get_partner, save_partner, delete_partner,
             register_connection, register_control_sample, register_malformed_state, close

lower-level classes
-------------------

.. autodata:: DatabaseObject

   sqlalchemy declarative base for :class:`SuccessfulSamplesSummary` and :class:`FailedSample`

.. autoclass:: SuccessfulSamplesSummary
   :members: partner_id, interval, samples,
             __init__

.. autoclass:: FailedSample
   :members: partner_id, interval, webfinger_address,
             __init__

.. autoclass:: ControlSampleCache
   :members: __init__, add_successful_sample, add_failed_sample, count_successful_samples, count_failed_samples, cleanup, clear, close
