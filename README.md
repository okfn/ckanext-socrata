# ckanext-socrata

This extension provides a CKAN Harvest plugin that consumes metadata from Socrata data catalogs, using the [Socrata Discovery API](https://socratadiscovery.docs.apiary.io).

## Installation

1.  Install ckanext-harvest ([https://github.com/ckan/ckanext-harvest#installation](https://github.com/ckan/ckanext-harvest#installation))

2.  Install the extension on your virtualenv:

        (pyenv) $ pip install -e git+https://github.com/ckan/ckanext-socrata.git#egg=ckanext-socrata

3.  Install the extension requirements:

        (pyenv) $ pip install -r ckanext-socrata/requirements.txt

4.  Enable the required plugins in your ini file:

        ckan.plugins = harvest socrata_harvester

## Usage

Create a new harvest source of type "Socrata" and enter the URL of the Socrata catalog you want to harvest from. The default base url to retrieve catalogues is "https://api.us.socrata.com/api/catalog/v1". You can provide a config object to the harvester to change this base url. For example:

```json
{
  "base_url": "https://api.eu.socrata.com/api/catalog/v1"
}
```

For local development, run

```bash
ckan harvester gather-consumer
ckan harvester fetch-consumer
```

to see the harvest jobs being processed.

## Copying and License

This material is copyright (c) Open Knowledge International.

It is open and licensed under the GNU Affero General Public License (AGPL) v3.0 whose full text may be found at:

http://www.fsf.org/licensing/licenses/agpl-3.0.html
