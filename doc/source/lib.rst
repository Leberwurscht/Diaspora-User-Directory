library with helper classes and functions
=========================================

.. py:module:: sduds.lib

This package contains some small auxiliary classes and functions that can be separated well from all the rest. This is where they are used:

* The :mod:`~sduds.lib.authentication` module is used in the :meth:`Application.synchronize_with_partner() <sduds.application.Application.synchronize_with_partner>` method and the :class:`~sduds.application.SynchronizationRequestHandler` class: When one server wants to synchronize with another, it must authenticate to it. This module implements server and client side of authentication with a simple interface.

* The :mod:`~sduds.lib.scheduler` module is used in the :meth:`~sduds.application.Application.configure_jobs` method to automate synchronizing with other server and to run database cleanup jobs regularly. It is also used in the :mod:`manage_partners` program to validate the cron-like syntax of the synchronization schedules entered by the admin.

* The :mod:`~sduds.lib.signature` module is used in :meth:`Profile.assert_validity <sduds.states.Profile.assert_validity>` to verify the signatures of the CAPTCHA provider. The module implements also a function to create signatures, which is solely used for the tests.

* The :mod:`~sduds.lib.sqlalchemyExtensions` module is used for mapping the :class:`~sduds.partners.Partner`, :class:`~sduds.partners.ControlSample`, :class:`~sduds.partners.Violation` and :class:`~sduds.states.State` classes to database tables: All of these classes have string attributes, these must be mapped using two custom column types. Moreover, the :class:`~sduds.states.State` class has a calculated property called :attr:`~sduds.states.State.hash` which should also be mapped to a column in order to be used in database queries. To achieve this, an sqlalchemy extension for calculated properties is implemented.

* The :mod:`~sduds.lib.threadingserver` module is used by the :class:`~sduds.application.SynchronizationServer` class, which is just a :class:`threading.Thread` that wraps around the :class:`~sduds.lib.threadingserver.ThreadingServer` class defined in this module.

List of submodules
------------------

.. toctree::
   :maxdepth: 1

   lib/authentication
   lib/scheduler
   lib/signature
   lib/sqlalchemyExtensions
   lib/threadingserver
