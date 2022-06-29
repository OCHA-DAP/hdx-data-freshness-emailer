# Summary

The HDX freshness emailer reads the HDX data freshness database and finds datasets whose status has changed. 
It sends emails to system administrators if the change is from overdue to delinquent or to maintainers if 
from due to overdue. It also alerts when there are new candidates for the data grid and reports datasets that 
are broken or which have invalid maintainers. An email is also sent for organisations with invalid 
administrators. 

# Information

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.

The code for the library is [here](https://github.com/OCHA-DAP/hdx-data-freshness-emailer).
The library has detailed API documentation which can be found in the menu at the top. 

# Usage

    python run.py
