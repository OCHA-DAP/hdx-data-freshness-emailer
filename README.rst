HDX Data Freshness Emailer
~~~~~~~~~~~~~~~~~~~~~~~~~~

|Build Status| |Coverage Status|

The HDX freshness emailer in Python reads the HDX data freshness database and finds datasets whose status has changed.
It sends emails to system administrators if the change is from overdue to delinquent or to maintainers if from due to overdue.

Usage
~~~~~

::

    python run.py

.. |Build Status| image:: https://travis-ci.org/OCHA-DAP/hdx-data-freshness-emailer.svg?branch=master&ts=1
   :target: https://travis-ci.org/OCHA-DAP/hdx-data-freshness-emailer
.. |Coverage Status| image:: https://coveralls.io/repos/github/OCHA-DAP/hdx-data-freshness-emailer/badge.svg?branch=master&ts=1
   :target: https://coveralls.io/github/OCHA-DAP/hdx-data-freshness-emailer?branch=master
