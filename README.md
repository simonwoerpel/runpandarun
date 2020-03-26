# datastore

A simple interface for reproducible & persistent data warehousing around small
data analysis projects with [`pandas`](https://pandas.pydata.org/). Currently
supports `csv` and `json` files.

- Manage different datasets from different sources, store them on disk,
have them versioned by fetch timestamp or incrementally append new data with
each update.
- Turn them into comparable/joinable
  [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)
  objects, apply some general cleaning, and start your analysis.
- Quick & easy combining of datasets via `pandas.concat`
- Simple command-line interface, e.g. for updating via cronojobs

## Quickstart

Specify your datasets via `yaml` syntax:

```python
from datastore import Datastore

config = """
datasets:
  my_dataset:
    csv_url: http://example.org/data.csv  # url to fetch csv file from
    columns:                              # only use specified columns
      - id: identifier                    # rename original column `identifier` to `id`
      - name
      - date
    dt_index: date                        # set date-based index
  another_dataset: ...
"""

store = Datastore.from_yaml_string(config)

df = store.my_dataset.df   # access `pandas.DataFrame`
df['name'].plot.hist()

other = store.another_dataset
other.daily.mean().plot()  # some handy shorthands for pandas

# do your beloved pandas stuff...
```

Organize persistence config and state of datasets:

```python
from datastore import Datastore

store = Datastore.from_yaml('./path/to/datasets.yml')
dataset = store.my_dataset

# update data from remote source:
dataset = dataset.update()

# update complete store:
store.update()
```

**TODO**: Update your datastore from the command-line (for use in cronjobs e.g.)

Update all:

    $ datastore update --config /path/to/config.yml

Only specific datasets:

    $ datastore update --config /path/to/config.yml --datasets my_dataset my_other_dataset ...

## Installation

Requires python3. Virtualenv use recommended.

Additional dependencies (`pandas` et. al.) will be installed automatically:

    pip install git+https://github.com/simonwoerpel/simple-datastore.git#egg=datastore

After this, you should be able to:

```python
from datastore import Datastore

# start the party...
```

## Config

The yaml config can be loaded either as string, from file or directly as `dict`
passed in:

    Datastore.from_yaml_string('...')
    Datastore.from_yaml('./path/to/config.yml')
    Datastore.from_dict(config_dict)


See [./examples/](./examples/)

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

```yaml
    index: id
    dt_index: true                  # optional specify that it should be a date/time-based index
```

**Sort**

- *optional*
- specify arguments for
  [`pandas.DataFrame.sort_values()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html)
- default: [`pandas.DataFrame.sort_index()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_index.html)

```yaml
    sort:                           # pass parameters for pandas function `sort_values`
      by:
        - column1
        - column2
      ascending: false
```

**De-duplicate**

- *optional*
- specify arguments for de-duplication
- default: [`pandas.DataFrame.drop_duplicates()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)
- when using a subset, use in conjunction with the `sort` directive to make
  sure to keep the right records

```yaml
    drop_duplicates:                # pass parameters for pandas function `drop_duplicates`
      subset:
        - column1
        - column2
      keep: last

    ...
    drop_duplicates: false          # disable default de-duplication
```

### combining

A quick top-level option for easy combining datasets from different sources.

This happens via
[`pandas.concat`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html)
and decides if it should concat *long* or *wide* (aka `pandas.concat(..., axis=1)`)

- **long**: if index name and column names accross the specified datasets in config `combine` are the same
- **wide**: if index is the same accross the specified datasets in config `combine`

TODO: *more to come... (aka merging)*

## Usage in your scripts

Once set up, you can start moving the data warehousing out of your analysis
scripts and focus on the analysis itself :upside_smiling_face:

```python
from datastore import Datastore

store = Datastore.from_yaml(config)

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

s = dataset.yearly.sum()
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

## developement

Install testing requirements:

    make install

Test:

    make test
