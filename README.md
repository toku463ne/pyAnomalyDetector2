# pyAnomalyDetector

## Setup
**Prepare postgresql server**

**Execute the setup script**
```bash
./setup.sh
```

## Setup postgresql for admin
```sql
CREATE DATABASE anomdec;
CREATE DATABASE anomdec_test;
CREATE USER anomdec WITH ENCRYPTED PASSWORD 'anomdec_pass';
GRANT ALL PRIVILEGES ON DATABASE anomdec TO anomdec;
GRANT ALL PRIVILEGES ON DATABASE anomdec_test TO anomdec;
```
  
## How to use  
1. Collect trend data



## Algorithm
- load config:
load `default.yml` and an additional config file if provided

- Initialize or update trends data:
    Get `trends` from the data source and save the following values to `anomdec.trends_stats` table.  
	  - sum: sum of values
      - sqr_sum: sum of square of values
      - count: count of values
      - t_mean: average
      - t_std: standard deviation
  
- Data Conversion:
    Convert `history` from the data source into the `anomdec.history` table.  
    Data will drop into a single interval defined in the config file.  
  
  
- 1st detection:
    - calculate h_mean: the mean of each items in the `anomdec.history` table
    - calculate lambda1: (h_mean - t_mean)/t_std  of each items
    - if lambda1 > lambda1_threshold, store the item in the dict variable latest_items
    
- 2nd detection:
  for items in latest_items variable
	- Get `trends` from the data source filtering by
		lambda2: (value - t_mean)/std > lambda2_threshold
	- calculate t2_mean, t2_std of the filtered values
    - calculate lambda3: (h_mean - t2_mean)/t2_std
	- if lambda3 > lambda3_threshold, store the item in the dict variable latest_items
	
- Normalize recent history:
    - For item in latest_items:
      - Normalize item data in `anomdec.history` so that max=1 and min=-0

- Summarize recent data:
    - classify the normalized data by a customized k-means algorithm  

- View the result
	- Show all items filtered by 2nd detection somehow (zabbix dashboard etc)  
  
- Alarming:
	- If there are items from multiple hosts in the same k-means group, send an alarm.
    
