# Integrate Data
Load data from different data sources, listed below
## ENTSO-E
https://transparency.entsoe.eu/

#### Requirements
Data Access

1. Register on the ENTSO-E transparency platform (click login at the top right of the page)  
2. Request an API key by sending an email to transparency@entsoe.eu with “Restful API access” in the subject line. In the email body state your registered email address.  
3. Install the Python client for the ENTSO-E platform ENTSO-E Python Client ```pip install entsoe-py```
4. Add your API-key src/data/entsoe_api.txt

See API-methods available  
https://github.com/EnergieID/entsoe-py

#### How to run
``` bash
$ python make_data_entsoe.py method date_from date_to
```
example
``` bash
$ python make_data_entsoe.py load_day_ahead 20220101 20230101
```

## ENTSO-G
https://transparency.entsog.eu/  
Not implemented

## Investing.com
https://www.investing.com/commodities/energy  
Manual download CSV
