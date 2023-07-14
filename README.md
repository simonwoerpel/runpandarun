# Run Panda Run

:panda_face: :panda_face: :panda_face: :panda_face: :panda_face: :panda_face: :panda_face:

A simple interface written in python for reproducible i/o workflows around tabular data via [pandas](https://pandas.pydata.org/) `DataFrame` specified via `yaml` "playbooks".

**NOTICE**

As of july 2023, this package only handles pandas transform logic, no data warehousing anymore.  See [archived version](https://github.com/simonwoerpel/runpandarun)

## Quickstart

[Install via pip](#installation)

Specify your operations via `yaml` syntax:

```yaml
read:
  uri: ./data.csv
  options:
    skiprows: 3

operations:
  - handler: DataFrame.rename
    options:
      columns:
        value: amount
  - handler: Series.map
    column: slug
    options:
      func: "lambda x: normality.slugify(x) if isinstance(x) else 'NO DATA'"
```

store this as a file `pandas.yml`, and apply a data source:

    cat data.csv | runpandarun pandas.yml > data_transformed.csv

Or, use within your python scripts:

```python
from runpandarun import Playbook

play = Playbook.from_yaml("./pandas.yml")
df = play.run()  # get the transformed dataframe

# change playbook parameters on run time:
play.read.uri = "s3://my-bucket/data.csv"
df = play.run()
df.to_excel("./output.xlsx")

# the play can be applied directly to a data frame,
# this allows more granular control
df = get_my_data_from_somewhere_else()
df = play.run(df)
```

## Installation

Requires at least python3.10 Virtualenv use recommended.

Additional dependencies (`pandas` et. al.) will be installed automatically:

    pip install runpandarun

After this, you should be able to execute in your terminal:

    runpandarun --help

## Reference

The playbook can be programmatically obtained in different ways:

```python
from runpandarun import Playbook

# via yaml file
play = Playbook.from_yaml('./path/to/config.yml')

# via yaml string
play = Playbook.from_string("""
operations:
- handler: DataFrame.sort_values
  options:
    by: my_sort_column
""")

# directly via the Playbook object (which is a pydantic object)
play = Playbook(operations=[{
    "handler": "DataFrane.sort_values",
    "options": {"by": "my_sort_column"}
}])
```

All options within the Playbook are optional, if you apply an empty play to a DataFrame, it will just remain untouched (but `runpandarun` won't break)

The playbook has three sections:

- read: instructions for reading in a source dataframe
- operations: a list of functions with their options (kwargs) executed in the given order
- write: instructions for saving a transformed dataframe to a target

### Read and write

Under the hood, `runpandarun` uses [smart_open](https://github.com/RaRe-Technologies/smart_open), so additionally to `stdin` / `stdout`, the input and output locations can be anything that `smart_open` can read and write to, like:

```
s3://my_bucket/data.csv
gs://my_bucket/data.csv
azure://my_bucket/data.csv
hdfs:///path/data.csv
hdfs://path/data.csv
webhdfs://host:port/path/data.csv
./local/path/data.csv
./local/path/data.csv.gz
file:///home/user/file.csv
file:///home/user/file.csv.bz2
[ssh|scp|sftp]://username@host//path/file.csv
[ssh|scp|sftp]://username@host/path/file.csv
[ssh|scp|sftp]://username:password@host/path/file.csv
```

And, of course, just `http[s]://...`

So, for example, you could transform a source from `s3` to a `sftp` endpoint:

    runpandarun pandas.yml -i s3://my_bucket/data.csv -o sftp://user@host/data.csv

#### Yaml spec

you can overwrite the `uri` arguments in the command line with `-i / --in-uri` and `-o / --out-uri`

More information about handlers and their options: [Pandas IO tools](https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html)

```yaml
read:
  uri: s3://my-bucket/data.xls  # input uri, anything that smart_open can read
  handler: read_excel           # default: guess by file extension, fallback: read_csv
  options:                      # options for the handler
    skiprows: 2

write:
  uri:                          # output uri, anything that smart_open can write to
  handler: write_excel          # default: guess by file extension, fallback: write_csv
  options:                      # options for the handler
    index: false
```

### Operations

The `operations` key of the yaml spec holds the transformations that should be applied to the data in order.

An operation can be any function from [pd.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) or [pd.Series](https://pandas.pydata.org/pandas-docs/stable/reference/series.html). Refer to these documentations to see their possible options (as in `**kwargs`).

For the handler, specify the module path without a `pd` or `pandas` prefix, just `DataFrame.<func>` or `Series.<func>`. When using a function that applies to a `Series`, tell :panda_face: which one to use via the `column` prop.

```yaml
operations:
  - handler: DataFrame.rename
    options:
      columns:
        value: amount
```

This exactly represents this python call to the processed dataframe:

```python
df.rename(columns={"value": "amount"})
```


### env vars

For api keys or other secrets, you can put environment variables anywhere into the config. They will simply resolved via `os.path.expandvars`

```yaml
read:
  options:
    storage_options:
      header:
        "api-key": ${MY_API_KEY}
```

## Example

A full playbook example that covers a few of the possible cases.

See the yaml files in [./tests/fixtures/](./tests/fixtures/) for more.

```yaml
read:
  uri: https://api.example.org/data?format=csv
  options:
    storage_options:
      header:
        "api-key": ${API_KEY}
    skipfooter: 1

operations:
  - handler: DataFrame.rename
    options:
      columns:
        value: amount

  - handler: Series.str.lower
    column: state

  - handler: DataFrame.assign
    options:
      city_id: "lambda x: x['state'] + '-' + x['city'].map(normality.slugify)"

  - handler: DataFrame.set_index
    options:
      keys:
        - city_id

  - handler: DataFrame.sort_values
    options:
      by:
        - state
        - city

write:
  uri: ftp://user:${FTP_PASSWORD}@host/data.csv
  options:
    index: false
```

## How to...

### Rename columns

[`DataFrame.rename`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html)

```yaml
operations:
  - handler: DataFrame.rename
    options:
      columns:
        value: amount
        "First name": first_name
```

### Apply modification to a column

[`Series.map`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.map.html)

```yaml
operations:
  - handler: Series.map
    column: my_column
    options:
      func: "lambda x: x.lower()"
```

### Set an index

[`DataFrame.set_index`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.set_index.html)

```yaml
operations:
  - handler: DataFrame.set_index
    options:
      keys:
        - city_id
```

### Sort values

[`DataFrame.sort_values`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html)

```yaml
operations:
  - sort_values:
      by:
        - column1
        - column2
      ascending: false
```

### De-duplicate

[`DataFrame.drop_duplicates`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)

when using a subset of columns, use in conjunction with `sort_values` to make sure to keep the right records

```yaml
operations:
  - drop_duplicates:
      subset:
        - column1
        - column2
      keep: last
```

### Compute a new column based on existing data

[`DataFrame.assign`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.assign.html)

```yaml
operations:
  - handler: DataFrame.assign
    options:
      city_id: "lambda x: x['state'] + '-' + x['city'].map(normality.slugify)"
```

## save eval

Ok wait, you are executing arbitrary python code in the yaml specs?

Not really, there is a strict allow list of possible modules that can be used. See [runpandarun.util.safe_eval](https://github.com/investigativedata/runpandarun/blob/develop/runpandarun/util.py)

This includes:
- any pandas or numpy modules
- [normality](https://github.com/pudo/normality/)
- [fingerprints](https://github.com/alephdata/fingerprints)

So, this would, of course, **NOT WORK** ([as tested here](https://github.com/investigativedata/runpandarun/blob/develop/tests/test_playbook.py))

```yaml
operations:
  - handler: DataFrame.apply
    func: "__import__('os').system('rm -rf /')"
```


## development

Package is managed via [Poetry](https://python-poetry.org/)

    git clone https://github.com/investigativedata/runpandarun

Install requirements:

    poetry install --with dev

Test:

    make test

## Funding

Since July 2023, this project is part of [investigraph](https://investigraph.dev) and development of this project is funded by

[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>
