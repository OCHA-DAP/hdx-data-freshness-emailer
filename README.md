[![Build Status](https://github.com/OCHA-DAP/hdx-data-freshness-emailer/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-data-freshness-emailer/actions?query=workflow%3Abuild)
[![Coverage Status](https://codecov.io/gh/OCHA-DAP/hdx-data-freshness-emailer/branch/main/graph/badge.svg?token=JpWZc5js4y)](https://codecov.io/gh/OCHA-DAP/hdx-data-freshness-emailer)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

The HDX freshness emailer reads the HDX data freshness database and finds datasets whose status has changed. 
It sends emails to system administrators if the change is from overdue to delinquent or to maintainers if 
from due to overdue. It also alerts when there are new candidates for the data grid and reports datasets that 
are broken or which have invalid maintainers. An email is also sent for organisations with invalid 
administrators. 

For more information, please read the [documentation](https://hdx-data-freshness-emailer.readthedocs.io/en/latest/). 

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.
