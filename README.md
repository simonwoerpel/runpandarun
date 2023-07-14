# Run Panda Run

:panda_face: :panda_face: :panda_face: :panda_face: :panda_face: :panda_face: :panda_face:

A simple interface written in python for reproducible i/o workflows around tabular data via [`pandas`](https://pandas.pydata.org/) `DataFrame` specified via `yaml` "playbooks".

Currently supports `[c|t]sv`, `xls[x]` and `json` input data. Output is always `csv`

**NOTICE**

As of july 2023, this package only handles pandas transform logic, no data warehousing anymore.  See [archived version](https://github.com/simonwoerpel/runpandarun)

## Quickstart

[Install via pip](#installation)

Specify your operations via `yaml` syntax:

```yaml
columns:                              # only use specified columns
  - id: identifier                    # rename original column `identifier` to `id`
  - name
  - date
dt_index: date                        # set date-based index
```

store this as a file `pandas.yml`, and apply a data source:

    cat data.csv | runpandarun pandas.yml > data_transformed.csv

Or, use within your python scripts:

```python
from runpandarun import Playbook

play = Playbook.from_yaml("./pandas.yml")
play.run(in_uri="./data.csv", out_uri="./output.csv")

# if you want to export the data into another format, do it manually:
df = play.run(in_uri="./data.csv")
df.to_excel("./output.xlsx")

# the play can of course be applied directly to a data frame:
df = play.run(df)
```

## Reference

### Input and output locations

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

## Installation

Requires at least python3.10 Virtualenv use recommended.

Additional dependencies (`pandas` et. al.) will be installed automatically:

    pip install runpandarun

After this, you should be able to execute in your terminal:

    runpandarun --help

## Playbook

The playbook can be programmatically obtained in different ways:

```python
from runpandarun import Playbook

# via yaml file
play = Playbook.from_yaml('./path/to/config.yml')

# via yaml string
play = Playbook.from_string("""
columns:
- id: RegisterNr
- column1
index: foo
""")

# directly via the Playbook object (which is a pydantic object)
play = Playbook(columns=[
    "column1",
    {"id": "RegisterNr"},
}])
```

### examples

See the yaml files in [./tests/fixtures/](./tests/fixtures/)

### playbook spec

All options within the Playbook are optional, if you apply an empty play to a DataFrame, it will just remain untouched (but `runpandarun` won't break)

#### Source and target

optional, `cli` arguments override this.

```yaml
in_uri:             # input uri of json or tabular source, anything that smart_open can read
out_uri:            # output uri, anything that smart_open can write to
```

#### Request params

If the `in_uri` is a http source, you can pass `params` and `headers` that will feed into [`requests.get()`](https://requests.readthedocs.io/en/master/user/quickstart/#make-a-request)

```yaml
in_uri: https://example.org/data.csv
request:
  params:
    format: csv
  header:
    "api-key": 123abc
```

##### env vars

For api keys or other secrets, you can put environment variables into the config. They will simply resolved via `os.path.expandvars`

```yaml
request:
  header:
    "api-key": ${MY_API_KEY}
```

#### Columns

specify list of subset columns to use

```yaml
columns:
  - column1
  - column2: origColumnName     # optional renaming mapping (rename `origColumnName` to `column2`)
```

#### Index

specify which column (after renaming was applied) should be the index. Otherwise, pandas default index is used.

```yaml
index: person_id                # set column `person_id` as index
```

```yaml
dt_index: event_date            # specify a date/time-based index instead
```

you can provide additional params to `pd.DateTimeIndex`:

```yaml
dt_index:
  column: event_date
  format: "%d.%m.%Y"
```

### Operations

Apply [any valid operation that is a function attribute of `pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) (like `drop_duplicates`, `sort_values`, `fillna` ...) in the given order with optional function arguments that will be passed to the call.

Here are examples:

#### Sort

[`pandas.DataFrame.sort_values()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html)

```yaml
ops:
  - sort_values:                    # pass parameters for pandas function `sort_values`
      by:
        - column1
        - column2
      ascending: false
```

#### De-duplicate

[`pandas.DataFrame.drop_duplicates()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)

when using a subset of columns, use in conjunction with `sort_values` to make sure to keep the right records

```yaml
ops:
  - drop_duplicates:              # pass parameters for pandas function `drop_duplicates`
      subset:
        - column1
        - column2
      keep: last
```

## development

Package is managed via [Poetry](https://python-poetry.org/)

    git clone https://github.com/investigativedata/runpandarun

Install requirements:

    poetry install --with dev

Test:

    make test
