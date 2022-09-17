# GovTech TAP 2022 Data Science Challenge

### Key Assumptions
* Total Premium is derived from monthly premium and months insured.
* Missing columns for certains attributes are meaningful.
    * i.e: NaN for riders is equivalent to no riders applicable for the insuree

### Directory
* insurance.ipynb: Notebook for Data Analysis and Modelling
* data_push.py: Python file for pushing data with destination structure to mongo db

### Setup Commands
* `py -m pip install -r requirements.txt`
* `docker-compose up -d` to run mongodb instance
* `py data_push.py` to run data mapping and insertion script