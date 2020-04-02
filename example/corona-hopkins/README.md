# corona-hopkins-example

:panda_face:

How to obtain & manage the worldwide corona data from Hopkins University with
[runpandarun](https://github.com/simonwoerpel/runpandarun)

## config

```yaml
storage:
  data_root: ./hopkins-data/
publish:
  public_root: /var/www/default/
  name: cleaned
  overwrite: true
datasets:
  hopkins:
    csv_url: https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date}.csv
    date_loop:
      start: 01-22-2020
      interval: 1D
    update: true
    incremental: true
    columns:
      country:
        - Country/Region
      province:
        - Province/State
      confirmed:
        - Confirmed
      deaths:
        - Deaths
      recovered:
        - Recovered
      last_updated:
        - Last Update
    dt_index: last_updated
```

## usage

python3, virtualenv recommended

        pip install runpandarun

copy the config into your project, and load it:

```python
from runpandarun import Datastore

store = Datastore.from_yaml('./path/to/config.yml')

# first run will fetch all
store.update()

# next run only if today's date is greater than last stored date
store.update()

# force update for historical data
store.update(fetch_all=True)

# publish wrangled data
store.publish()

# or, do somethin with the hopkins data and publish afterwards:
hopkins = store.hopkins
df = do_magic_wrangle(hopkins.df)
hopkins.publish(df)
```

## cli / automate via cronjob

        $ runpandarun update --config ./path/to/config.yml
