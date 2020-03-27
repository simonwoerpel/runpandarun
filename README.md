# Run Panda Run

:panda_face: :panda_face: :panda_face: :panda_face: :panda_face: :panda_face: :panda_face:

A simple interface written in python for reproducible & persistent data
warehousing around small data analysis / processing projects with
[`pandas`](https://pandas.pydata.org/).

Currently supports `csv` and `json` resources.

Useful to build an automated workflow like this:

**1. Download** fetch datasets from different remote or local sources, store
them somewhere (with respect to versioning / incremental updates), do some
general cleaning, [processing](#operations) and get a nice
[`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)
for each dataset and

**2. Wrangle** your data somehow

[**3. Publish**](#publish) some of the wrangled data somewhere where other
services (Google Spreadsheet, Datawrapper, even another `Datastore`) can work
on further

It includes a simple command-line interface, e.g. for automated
processing via cronjobs.

## Quickstart

[Install via pip](#installation)

Specify your datasets via `yaml` syntax:

```python
from runpandarun import Datastore

config = """
datasets:
  my_dataset:
    csv_url: http://example.org/data.csv  # url to fetch csv file from
    columns:                              # only use specified columns
      - id: identifier                    # rename original column `identifier` to `id`
      - name
      - date
    dt_index: date                        # set date-based index
  another_dataset:
    json_url: !ENV ${SECRET_URL}          # see below for env vars
    ...
"""

store = Datastore(config)

df = store.my_dataset.df   # access `pandas.DataFrame`
df['name'].plot.hist()

other = store.another_dataset
other.daily.mean().plot()  # some handy shorthands for pandas

# do your beloved pandas stuff...
```

Organize persistence config and state of datasets:

```python
from runpandarun import Datastore

store = Datastore('./path/to/datasets.yml')
dataset = store.my_dataset

# update data from remote source:
dataset = dataset.update()

# update complete store:
store.update()
```

Update your datastore from the command-line (for use in cronjobs e.g.)

Specify config path either via `--config` or env var `CONFIG`

Update all:

    runpandarun update --config /path/to/config.yml

Only specific datasets and with env var:

    CONFIG=/path/to/config.yml runpandarun update my_dataset my_other_dataset ...

## Installation

Requires python3. Virtualenv use recommended.

Additional dependencies (`pandas` et. al.) will be installed automatically:

    pip install git+https://github.com/simonwoerpel/runpandarun.git#egg=runpandarun

After this, you should be able to execute in your terminal:

    runpandarun -h

You should as well be able to import it in your python scripts:

```python
from runpandarun import Datastore

# start the party...
```

## Config

The yaml config can be loaded either as string, from file or directly as `dict`
passed in:

```python
store = Datastore('./path/to/config.yml')
store = Datastore(config_dict)
store = Datastore("""
    datasets:
      my_dataset:
      ...
""")
```

To quickly test your config, you can use the command-line:

    CONFIG=config.yml runpandarun print my_dataset

See [./example/](./example/)

### top-level options

```yaml
storage:
  data_root: ./path/    # absolute or relative path where to store the files
combine:
  - dataset1            # keys of defined datasets for quick merging
  - dataset2
datasets:               # definition for datasets
  dataset1:
    csv_url: ...
```

### dataset options

**Source link**

- *required*
- any of:

```yaml
    csv_url:            # url to remote csv, the response must be the direct csv content
                        # this can also be a Google Spreadsheet "published to the web" in csv format
    csv_local:          # absolute or relative path to a file on disk
    json_url:           # url to remote json, the response should be text or application/json
    json_local:         # absolute or relative path to a file on disk
```

**Request params**

- *optional*
- for each source url, you can pass `params` and `headers` that will feed into
  [`requests.get()`](https://requests.readthedocs.io/en/master/user/quickstart/#make-a-request)

```yaml
    csv_url: https://example.org
    request:
      params:
        format: csv
      header:
        "api-key": 123abc
```

**Incremental**

- *optional*
- instead of versioning the downloaded datasets and only use the latest one,
  this flag allows to combine all the downloaded data over time to one dataset.
  Use case example: A publisher releases updated data each day under the same url

```yaml
    incremental: true
```

**Columns**

- *optional*
- specify list of subset columns to use

```yaml
    columns:
      - column1
      - column2: origColumnName     # optional renaming mapping (rename `origColumnName` to `column2`)
```

**Index**

- *optional*
- specify which column (after renaming was applied) should be the index
- default: `id`

```yaml
    index: person_id                # set column `person_id` as index
```

```yaml
    dt_index: event_date            # specify a date/time-based index instead
```

### Operations

- *optional*

Apply [any valid operation that is a function attribute of `pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html)
(like `drop_duplicates`, `sort_values`, `fillna` ...) in the given order with optional
function arguments that will be passed to the call.

Default operations: `['drop_duplicates', 'sort_index']`

Disable:
```yaml
    ...
    ops: false
```

Here are examples:

**Sort**

[`pandas.DataFrame.sort_values()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html)

```yaml
    ...
    ops:
      sort_values:                    # pass parameters for pandas function `sort_values`
        by:
          - column1
          - column2
        ascending: false
```

**De-duplicate**

[`pandas.DataFrame.drop_duplicates()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)
- when using a subset, use in conjunction with `sort_values` to make sure to keep the right records

```yaml
    ...
    ops:
      drop_duplicates:              # pass parameters for pandas function `drop_duplicates`
        subset:
          - column1
          - column2
        keep: last
```

### combining

A quick top-level option for easy combining datasets from different sources.

This happens via
[`pandas.concat`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html)
and decides if it should concat *long* or *wide* (aka `pandas.concat(..., axis=1)`)

- **long**: if index name and column names accross the specified datasets in config `combine` are the same
- **wide**: if index is the same accross the specified datasets in config `combine`

TODO: *more to come... (aka merging)*


### env vars

For api keys or other secrets, you can put environment variables into the config:

```yaml
storage:
  data_root: !ENV '${DATA_ROOT}/data/'
datasets:
  google_places:
    json_url: https://maps.googleapis.com/maps/api/place/findplacefromtext/json
    request:
      params:
        key: !ENV ${GOOGLE_APY_KEY}
    ...
```

## Usage in your scripts

Once set up, you can start moving the data warehousing out of your analysis
scripts and focus on the analysis itself...

```python
from runpandarun import Datastore

store = Datastore(config)

# all your datasets become direct attributes of the store:
ds = store.my_dataset

# all your datasets have their computed (according to your config) `pandas.DataFrame` as attribute:
df = store.my_dataset.df

# get combined df (if specified in the config)
df = store.combined
```

### resampling

some time-based shorthands (if you have a `dt_index: true` in your config)
based on
[`pandas.DataFrame.resample`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html)

The resulting `pandas.DataFrame` will only have columns with numeric data in it.

```python
dataset = store.my_time_based_dataset

s = dataset.daily.mean()
s.plot()

s = dataset.yearly.count().cumsum()
s.plot()
```

Available time aggregations:
- minutely
- hourly
- daily
- weekly
- monthly
- yearly

Available aggregation methods:
- sum
- mean
- max
- min
- count

For more advanced resampling, just work on your dataframes directly...

## Publish

After the workflow is done, you can publish some (or all) results.

```python
dataset = store.my_dataset
df1 = do_something_with(dataset.df)
dataset.publish(df1, overwrite=True, include_source=True)
df2 = do_something_else_with(dataset.df)
dataset.publish(df2, name='filtered_for_europe', format='json')
```

For behaviour reasons the `overwrite`-flag must be set explicitly (during
function execution or in config, see below), otherwise it will raise if a
public file already exists. To avoid overwriting, set a different name.

The `publish()` parameters can be set in the config as well, either globally or
per dataset:

```yaml
publish:
  public_root: /path/to/a/dir/a/webserver/can/serve/
  include_source: true
...
datasets:
  my_dataset:
    ...
    publish:
      include_source: false
      format: json
      name: something
```

**TODO**: currently only storing to a filesystem on the same machine implemented.

But features could be:
- goolge spreadsheet
- ftp
- s3
- ...


## cli

```bash
usage: runpandarun [-h] [--loglevel LOGLEVEL] {update,print,publish} ...

positional arguments:
  {update,print,publish}
                        commands help: run `runpandarun <command> -h`

optional arguments:
  -h, --help            show this help message and exit
  --loglevel LOGLEVEL
```


## developement

Install testing requirements:

    make install

Test:

    make test
