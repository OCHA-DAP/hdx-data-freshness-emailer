HDX Data Freshness Emailer
~~~~~~~~~~~~~~~~~~~~~~~~~~

|Build Status| |Coverage Status|

The HDX freshness emailer in Python reads the HDX data freshness database and finds datasets whose status has changed.
It sends emails to system administrators if the change is from overdue to delinquent or to maintainers if from due to overdue.

Usage
~~~~~

::

    python run.py

.. |Build Status| image:: https://github.com/OCHA-DAP/hdx-data-freshness-emailer/workflows/build/badge.svg
   :target: https://github.com/OCHA-DAP/hdx-data-freshness-emailer/actions?query=workflow%3Abuild
.. |Coverage Status| image:: https://coveralls.io/repos/github/OCHA-DAP/hdx-data-freshness-emailer/badge.svg?branch=main&ts=1
   :target: https://coveralls.io/github/OCHA-DAP/hdx-data-freshness-emailer?branch=main
