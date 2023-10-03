# runpandarun

## CHANGELOG

### 0.3.2 (2023-10-03)

- Upgrade dependencies

### 0.3.0 (2023-08-01)

- Add [datapatch](https://github.com/pudo/datapatch)

### 0.2.5 (2023-07-24)

- Fix `read_json` for already opened str/bytes stream handler

### 0.2.4 (2023-07-19)

- Add sql support via [sqlalchemy](https://www.sqlalchemy.org/)

### 0.2.0 (2023-07-14) BREAKING CHANGES

This tool was archived, but now re-activated to be used as a plugin in [investigraph](https://github.com/investigativedata/investigraph).

Therefore all the data fetching & storing logic is dropped from this library, only the core functionality of specifying and executing `pandas.DataFrame` transform operations via a `yaml` specification.

Refer to the `README.md` of the current version to see what this library (still) can do and what not.

## Prior changelog (2020)

[These changes survived in the archived master branch](https://github.com/simonwoerpel/runpandarun/tree/master)

### 0.1.4

- Add `url_replace` feature for dynamically url rewriting

### 0.1.3

- Add option to resample paginated sources by download date format
- Small bugfixes


### 0.1.2

- Add pagination by offset logic
- Allow lambda functions as strings in yaml for column transformation and df operations
- Add option in yaml to set cache header for google cloud storage blobs
- Small bugfixes


### 0.1.1

- first release with basic functionality
