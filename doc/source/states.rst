states module
=============

.. automodule:: sduds.states

.. autoclass:: Profile
   :members: full_name, hometown, country_code, services, captcha_signature, submission_timestamp,
             __init__, check, retrieve

.. autoclass:: State
   :members: address, retrieval_timestamp, profile, hash,
             __init__, check, retrieve

.. autoclass:: Ghost
   :members: hash, retrieval_timestamp,
             __init__

.. autoclass:: RetrievalFailed
.. autoclass:: CheckFailed
.. autoclass:: MalformedProfileException
.. autoclass:: MalformedStateException
.. autoclass:: RecentlyExpiredProfileException
