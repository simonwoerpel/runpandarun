Run Panda Run
=============

    
 

A simple interface written in python for reproducible & persistent data
warehousing around small data analysis / processing projects with
```pandas`` <https://pandas.pydata.org/>`__.

Currently supports ``csv`` and ``json`` resources.

Useful to build an automated workflow like this:

**1. Download** fetch datasets from different remote or local sources,
store them somewhere (with respect to versioning / incremental updates),
do some general cleaning, `processing <#operations>`__ and get a nice
```pandas.DataFrame`` <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`__
for each dataset and

**2. Wrangle** your data somehow, store and load
`revisions <#revisions>`__ to share *source of truth* between notebooks
or scripts.

`3. Publish <#publish>`__ some of the wrangled data somewhere where
other services (Google Spreadsheet, Datawrapper, even another
``Datastore``) can work on further

It includes a simple command-line interface, e.g. for automated
processing via cronjobs.

Quickstart
----------

`Install via pip <#installation>`__

Specify your datasets via ``yaml`` syntax:

.. code:: yaml

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

store this as a file, and set the env var ``CONFIG`` to the path:

::

       export CONFIG=./path/to/config.yml

.. code:: python

   from runpandarun.datasets import my_dataset, another_dataset

   df = my_dataset.get_df()
   df['name'].plot.hist()

   another_dataset.daily.mean().plot()  # some handy shorthands for pandas

Handle data persistence and state of datasets:

.. code:: python

   from runpandarun.datasets import my_dataset

   # update data from remote source:
   my_dataset = my_dataset.update()

   # update complete store:
   from runpandarun import Datastore

   store = Datastore()
   store.update()

   # save a revision
   df = my_dataset.get_df()
   df = wrangle(df)
   my_dataset.save(df, 'wrangled')

   # get this revision (in another script)
   df = my_dataset['wrangled']

   # publish
   df = my_dataset.get_df()
   clean(df)
   my_dataset.publish(df, name='cleaned', overwrite=True)

Update your datastore from the command-line (for use in cronjobs e.g.)

Specify config path either via ``--config`` or env var ``CONFIG``

Update all:

::

   runpandarun update --config /path/to/config.yml

Only specific datasets and with env var:

::

   CONFIG=/path/to/config.yml runpandarun update my_dataset my_other_dataset ...

Installation
------------

Requires python3. Virtualenv use recommended.

Additional dependencies (``pandas`` et. al.) will be installed
automatically:

::

   pip install runpandarun

After this, you should be able to execute in your terminal:

::

   runpandarun -h

You should as well be able to import it in your python scripts:

.. code:: python

   from runpandarun import Datastore

   # start the party...

Config
------

**Easy**

Set an environment variable ``CONFIG`` pointing to your yaml file.

``runpandarun`` will find your config and you are all set (see
`quickstart <#quickstart>`__)

**Manually**

Of course you can initialize the config manually:

-  from a file
-  as yaml string
-  as a python dict

.. code:: python

   from runpandarun import Datastore

   store = Datastore('./path/to/config.yml')
   store = Datastore(config_dict)
   store = Datastore("""
       datasets:
         my_dataset:
         ...
   """)

To quickly test your config for a dataset named ``my_dataset``, you can
use the command-line (this will print the generated csv to stdout):

::

   CONFIG=config.yml runpandarun print my_dataset

examples
~~~~~~~~

See the yaml files in `./example/ <./example/>`__

top-level options
~~~~~~~~~~~~~~~~~

.. code:: yaml

   storage:
     data_root: ./path/                    # absolute or relative path where to store the files
   publish:
     handlers:
       filesystem:
         public_root: !ENV ${PUBLIC_ROOT}  # where to store published data, e.g. a path to a webserver root via env var
         enabled: true
       gcloud:
         bucket: !ENV ${GOOGLE_BUCKET}     # or in a google cloud storage bucket...
         enabled: !ENV ${GOOGLE_PUBLISH}   # enable or disable a publish handler based on environment
   combine:
     - dataset1                            # keys of defined datasets for quick merging
     - dataset2
   datasets:                               # definition for datasets
     dataset1:
       csv_url: ...

dataset options
~~~~~~~~~~~~~~~

**Source link**

-  *required*
-  any of:

.. code:: yaml

       csv_url:            # url to remote csv, the response must be the direct csv content
                           # this can also be a Google Spreadsheet "published to the web" in csv format
       csv_local:          # absolute or relative path to a file on disk
       json_url:           # url to remote json, the response should be text or application/json
       json_local:         # absolute or relative path to a file on disk

**Request params**

-  *optional*
-  for each source url, you can pass ``params`` and ``headers`` that
   will feed into
   ```requests.get()`` <https://requests.readthedocs.io/en/master/user/quickstart/#make-a-request>`__

.. code:: yaml

       csv_url: https://example.org
       request:
         params:
           format: csv
         header:
           "api-key": 123abc

**Incremental**

-  *optional*
-  instead of versioning the downloaded datasets and only use the latest
   one, this flag allows to combine all the downloaded data over time to
   one dataset. Use case example: A publisher releases updated data each
   day under the same url

.. code:: yaml

       incremental: true

**Columns**

-  *optional*
-  specify list of subset columns to use

.. code:: yaml

       columns:
         - column1
         - column2: origColumnName     # optional renaming mapping (rename `origColumnName` to `column2`)

**Index**

-  *optional*
-  specify which column (after renaming was applied) should be the index
-  default: ``id``

.. code:: yaml

       index: person_id                # set column `person_id` as index

.. code:: yaml

       dt_index: event_date            # specify a date/time-based index instead

.. code:: yaml

       dt_index:
         column: event_date
         format: "%d.%m.%Y"

Operations
~~~~~~~~~~

-  *optional*

Apply `any valid operation that is a function attribute of
``pandas.DataFrame`` <https://pandas.pydata.org/pandas-docs/stable/reference/frame.html>`__
(like ``drop_duplicates``, ``sort_values``, ``fillna`` …) in the given
order with optional function arguments that will be passed to the call.

Default operations: ``['drop_duplicates', 'sort_index']``

Disable:

.. code:: yaml

       ...
       ops: false

Here are examples:

**Sort**

```pandas.DataFrame.sort_values()`` <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sort_values.html>`__

.. code:: yaml

       ...
       ops:
         sort_values:                    # pass parameters for pandas function `sort_values`
           by:
             - column1
             - column2
           ascending: false

**De-duplicate**

```pandas.DataFrame.drop_duplicates()`` <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html>`__
- when using a subset, use in conjunction with ``sort_values`` to make
sure to keep the right records

.. code:: yaml

       ...
       ops:
         drop_duplicates:              # pass parameters for pandas function `drop_duplicates`
           subset:
             - column1
             - column2
           keep: last

combining
~~~~~~~~~

A quick top-level option for easy combining datasets from different
sources.

This happens via
```pandas.concat`` <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html>`__
and decides if it should concat *long* or *wide* (aka
``pandas.concat(..., axis=1)``)

-  **long**: if index name and column names accross the specified
   datasets in config ``combine`` are the same
-  **wide**: if index is the same accross the specified datasets in
   config ``combine``

TODO: *more to come… (aka merging)*

env vars
~~~~~~~~

For api keys or other secrets, you can put environment variables into
the config:

.. code:: yaml

   storage:
     data_root: !ENV '${DATA_ROOT}/data/'
   datasets:
     google_places:
       json_url: https://maps.googleapis.com/maps/api/place/findplacefromtext/json
       request:
         params:
           key: !ENV ${GOOGLE_APY_KEY}
       ...

Usage in your scripts
---------------------

Once set up, you can start moving the data warehousing out of your
analysis scripts and focus on the analysis itself…

.. code:: python

   from runpandarun import Datastore

   store = Datastore(config)

   # all your datasets become direct attributes of the store:
   ds = store.my_dataset

   # all your datasets have their computed (according to your config) `pandas.DataFrame` as attribute:
   df = store.my_dataset.get_df()

   # get combined df (if specified in the config)
   df = store.combined

resampling
~~~~~~~~~~

some time-based shorthands (if you have a ``dt_index: true`` in your
config) based on
```pandas.DataFrame.resample`` <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html>`__

The resulting ``pandas.DataFrame`` will only have columns with numeric
data in it.

.. code:: python

   dataset = store.my_time_based_dataset

   s = dataset.daily.mean()
   s.plot()

   s = dataset.yearly.count().cumsum()
   s.plot()

Available time aggregations: - minutely - hourly - daily - weekly -
monthly - yearly

Available aggregation methods: - sum - mean - max - min - count

For more advanced resampling, just work on your dataframes directly…

Publish
-------

After the workflow is done, you can publish some (or all) results.

.. code:: python

   dataset = store.my_dataset
   df1 = do_something_with(dataset.get_df())
   dataset.publish(df1, overwrite=True, include_source=True)
   df2 = do_something_else_with(dataset.get_df())
   dataset.publish(df2, name='filtered_for_europe', format='json')

For behaviour reasons the ``overwrite``-flag must be set explicitly
(during function execution or in config, see below), otherwise it will
raise if a public file already exists. To avoid overwriting, set a
different name.

The ``publish()`` parameters can be set in the config as well, either
globally or per dataset, specified for each handler (currently
``filesystem`` or ``gcloud``). Dataset-specific settings overwrite
global ones for the storage handler.

.. code:: yaml

   publish:
     overwrite: true               # global option for all handlers: always overwrite existing files
     with_timestamp: true          # include current timestamp in filename
     handlers:
       filesystem:
         public_root: /path/to/a/dir/a/webserver/can/serve/
         include_source: true
       gcloud:
         bucket: my-bucket-name
         include_source: false
   ...
   datasets:
     my_dataset:
       ...
       publish:
         gcloud:
           bucket: another-bucket
           include_source: true
           format: json
           name: something

**TODO**: currently only storing to a filesystem or google cloud storage
implemented.

But features could be: - goolge spreadsheet - ftp - s3 - …

Revisions
---------

At any time between reading data in and publishing you can store and get
revisions of a dataset. This is usually a ``pd.DataFrame`` in an
intermediate state, e.g. after date enriching but before analysis.

This feature can be used in an automated processing workflow consisting
of multiple notebooks to share DataFrames between each other. The
underlying storage mechanism is
`pickle <https://docs.python.org/3/library/pickle.html>`__ to make sure
a DataFrame revision behaves as expected. This comes with the downside
that pickle’s are not safe to share between different systems, but to
re-create them in another environment, that’s what a reproducible
workflow is for, right?

**store a revision**

.. code:: python

   ds = store.my_dataset
   df = ds.get_df()
   ds.revisions.save('tansformed', df.T)

**load a revision**

.. code:: python

   ds = store.my_dataset
   df = ds['transformed']

**show available revisions**

.. code:: python

   ds = store.my_dataset
   ds.revisions.show()

**iterate through revisions**

.. code:: python

   ds = store.my_dataset
   for df in ds.revisions:
     do_something(df)

*Pro tip* you can go crazy and use this mechanism to store & retrieve
*any* object that is serializable via
`pickle <https://docs.python.org/3/library/pickle.html>`__

cli
---

.. code:: bash

   usage: runpandarun [-h] [--loglevel LOGLEVEL] {update,print,publish} ...

   positional arguments:
     {update,print,publish}
                           commands help: run `runpandarun <command> -h`

   optional arguments:
     -h, --help            show this help message and exit
     --loglevel LOGLEVEL

developement
------------

Install testing requirements:

::

   make install

Test:

::

   make test
