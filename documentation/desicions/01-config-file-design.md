# How to design the config file

## Status

In progress

## Context

Currently the project is as early as it can get with just a few insights. Data will be collected from various sources and will be stored in a database. The sources can be split into two categories: limited and unlimited. One source is able to provide one or more key figures, which are needed for the S&P 500 fields.</br>
There are several ways to design the config file, focusing on the called APIs or focusing on the S&P 500 constraints. Both should be able to prioritize certain S&P 500 fields.



## Options

Both options consist of a general part for database information and a special part for data gathering. The data gathering part can be customized in order to prioritize certain S&P 500 fields or exclude APIs.</br>
- The tasks will be executed in order of their priority. Meaning that lower is earlier. Tasks with the same priority will be executed in parallel.
- The <i>all</i> include argument will grab all possible values implemented for the API/field.
- The <i>exclude</i> allows to exclude fields/api's stated in the <i>include</i> config and will exclude it, if previously included.
- Tasks should be able to overwrite general parameters.

### Prioritize S&P 500 constraints

```
---
database_connection_string: myDB
database_user: root123
database_pw: secret
tasks:
- sp500_fields:
  - NYSE
  database_connection_string: overWrite DB
  priority: 1
  include_sources:
  - all
  exclude_sources: 
  - ''
- sp500_fields:
  - MARKET_CAP
  - MONTH_TRADING_VOLUME
  database_pw: another pw
  priority: 2
  include_sources:
  - all
  exclude_sources:
  - just_a_bit_brocken_API_1.com
...
```

### Prioritize APIs

```
---
database_connection_string: myDB
database_user: root123
database_pw: secret
tasks:
- api: mySource.com
  priority: 1.0
  include_s&p500_fields:
  - all
  exclude_s&p500_fields:
- api: location.com
  priority: 2.0
  include_s&p500_fields:
  - all
  exclude_s&p500_fields:
  - MONTH_TRADING_VOLUME_due_to_broken_json_interface
...
```


## Decision

The <i>priority</i> feature within the tasks gives non-hard coded control to the user of the processing order. This should help to quickly gather missing data of a single field, since the fields are weakly depending on each other, while the parallel execution gives more control about the chosen API.

The <i>include/exclude</i> feature gives control to the user to fine tune selected APIs or even exclude them, in case there are problems. 

Prioritizing the S&P 500 fields, will lead to a less volatile configuration file, since new APIs don't have to reflected in it. However, if the APIs are very instable a lot of including and excluding need to be done (if not detected automatically).

Prioritizing the APIs and setting them all to the same priority will maximize the bandwidth used and might be fastest way of collecting data. However, a lot of data might pile up, which is unnecessary and for each new API the configuration file needs to be changed.



## Consequences

Tasks which do not gather data are not yet considered.</br>
How credentials will be stored needs to be considered.</br>
Break up of proposed config schema is possible.