# :panda_face: examples :panda_face:

You only need the yaml files and [install runpandarun](https://github.com/simonwoerpel/runpandarun#installation)
to work with these examples.

- [google places api](#google-places-api)
- [rki corona scraper](#rki-corona-scraper)


## google places api


get a csv:

        $ CONFIG=google_api.yml runpandarun print places

Adjust the request params as described here: https://developers.google.com/places/web-service/search

[yaml file](./google_api.yml)

```yaml
datasets:
  places:
    json_url: https://maps.googleapis.com/maps/api/place/findplacefromtext/json
    request:
      params:
        key: INSERT KEY HERE  # will work on something to ues env vars here...
        language: de
        inputtype: textquery
        fields: formatted_address,name,place_id
        input: Supermarkt
    json_normalize:
      record_path: candidates
    columns:
      - formatted_address
      - name
      - id: place_id
```

## rki corona scraper

automatized download, cleaning and publish of RKI dashboard data without a line
of python (but bash):

        $ CONFIG=./rki_remote.yml runpandarun update rki
        $ CONFIG=./rki_remote.yml runpandarun publish rki

[yaml file](./rki_remote.yml)

```yaml
storage:
  data_root: ./data/
publish:
  public_root: ./data/public/
  name: rki
  overwrite: true
datasets:
  rki:
    json_url: https://services.arcgis.com/5T5nSi527N4F7luB/arcgis/rest/services/Historic_adm0_v3/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=OBJECTID%2CNewCase%2CDateOfDataEntry&orderByFields=DateOfDataEntry%20asc&resultOffset=0&resultRecordCount=2000&cacheHint=true
    # options for `pd.json_normalize`
    json_normalize:
      record_path: features
    columns:
      - id: attributes.RS
      - name: attributes.GEN
      - last_update: attributes.last_update
      - population: attributes.EWZ
      - cases: attributes.cases
      - deaths: attributes.deaths
      - death_rate: attributes.death_rate
      - cases_per_100k: attributes.cases_per_100k
      - state: attributes.BL
    dt_index:
      column: last_update
      # options for `pd.to_datetime`:
      format: "%d.%m.%Y %H:%M"
      errors: coerce
    # the data is updated, so we specify to include all downloaded data
    # but only show the latest data for each region via `sort_values` & `drop_duplicates`
    incremental: true
    ops:
      - sort_values:
          by:
            - id
            - last_update
          reverse: true
      - drop_duplicates:
          subset:
            - id
```
