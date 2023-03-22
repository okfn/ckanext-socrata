from __future__ import unicode_literals

import json
import uuid
import requests
from dateutil.parser import parse

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

try:
    from json import JSONDecodeError
except ImportError:
    from simplejson.scanner import JSONDecodeError



from ckan import model
from ckan.lib.munge import munge_tag
from ckan.plugins.core import implements
import ckan.plugins.toolkit as toolkit
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

import logging
log = logging.getLogger(__name__)

BASE_API_ENDPOINT = "https://api.us.socrata.com/api/catalog/v1"
DOWNLOAD_ENDPOINT_TEMPLATE = \
    "https://{domain}/api/views/{resource_id}/rows.csv?accessType=DOWNLOAD"


class SocrataHarvester(HarvesterBase):
    '''
    A CKAN Harvester for Socrata Data catalogues.
    '''
    implements(IHarvester)

    def _delete_dataset(self, id):
        base_context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True
        }
        # Delete package
        toolkit.get_action('package_delete')(base_context, {'id': id})
        log.info('Deleted package with id {0}'.format(id))

    def _get_existing_dataset(self, guid):
        '''
        Check if a dataset with an `identifier` extra already exists.

        Return a dict in `package_show` format.
        '''
        datasets = model.Session.query(model.Package.id) \
            .join(model.PackageExtra) \
            .filter(model.PackageExtra.key == 'identifier') \
            .filter(model.PackageExtra.value == guid) \
            .filter(model.Package.state == 'active') \
            .all()

        if not datasets:
            return None
        elif len(datasets) > 1:
            log.error('Found more than one dataset with the same guid: {0}'
                      .format(guid))

        return toolkit.get_action('package_show')({}, {'id': datasets[0][0]})

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_package_extra(self, pkg_dict, key):
        '''
        Helper function to retrieve the value from a package dict extra, given
        the key.
        '''
        for extra in pkg_dict.get('extras', []):
            if extra.get('key') == key:
                return extra.get('value')
        return None

    def _mark_datasets_for_deletion(self, guids_in_source, harvest_job):
        '''
        Given a list of guids in the remote source, check which in the DB need
        to be deleted. Query all guids in the DB for this source and calculate
        the difference. For each of these create a HarvestObject with the
        dataset id, marked for deletion.

        Return a list with the ids of the Harvest Objects to delete.
        '''

        object_ids = []

        # Get all previous current guids and dataset ids for this source
        query = \
            model.Session.query(HarvestObject.guid, HarvestObject.package_id) \
            .filter(HarvestObject.current == True) \
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)  # noqa

        guid_to_package_id = {}
        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = guid_to_package_id.keys()

        # Get objects/datasets to delete (ie in the DB but not in the source)
        guids_to_delete = set(guids_in_db) - set(guids_in_source)

        # Create a harvest object for each of them, flagged for deletion
        for guid in guids_to_delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status',
                                                           value='delete')])

            # Mark the rest of objects for this guid as not current
            model.Session.query(HarvestObject) \
                         .filter_by(guid=guid) \
                         .update({'current': False}, False)
            obj.save()
            object_ids.append(obj.id)

        return object_ids

    def _build_package_dict(self, context, harvest_object):
        '''
        Build and return a package_dict suitable for use with CKAN
        `package_create` and `package_update`.
        '''

        # Local harvest source organization
        source_dataset = toolkit.get_action('package_show')(
            context.copy(),
            {'id': harvest_object.source.id}
        )
        local_org = source_dataset.get('owner_org')

        res = json.loads(harvest_object.content)

        package_dict = {
            'title': res['resource']['name'],
            'name': self._gen_new_name(res['resource']['name']),
            'url': res.get('permalink', ''),
            'notes': res['resource'].get('description', ''),
            'author': res['resource']['attribution'],
            'tags': [],
            'extras': [],
            'identifier': res['resource']['id'],
            'owner_org': local_org,
            'resources': [],
        }

        # Add tags
        package_dict['tags'] = \
            [{'name': munge_tag(t)}
             for t in res['classification'].get('tags', [])
             + res['classification'].get('domain_tags', [])]

        # Add domain_metadata to extras
        package_dict['extras'].extend(res['classification']
                                      .get('domain_metadata', []))

        # Add source createdAt to extras
        package_dict['extras'].append({
            'key': 'source_created_at',
            'value': res['resource']['createdAt']
        })

        # Add source updatedAt to extras
        package_dict['extras'].append({
            'key': 'source_updated_at',
            'value': res['resource']['updatedAt']
        })

        # Add owner_display_name to extras
        package_dict['extras'].append({
            'key': 'owner_display_name',
            'value': res.get('owner', {}).get('display_name')
        })

        # Add categories to extras
        package_dict['extras'].append({
            'key': 'categories',
            'value': [t
                      for t in res['classification'].get('categories', [])
                      + res['classification'].get('domain_categories', [])],
        })

        # Add Socrata metadata.license if available
        if res['metadata'].get('license', False):
            package_dict['extras'].append({
                'key': 'license',
                'value': res['metadata']['license']
            })

        # Add provenance
        if res['resource'].get('provenance', False):
            package_dict['provenance'] = res['resource']['provenance']

        # Resources
        package_dict['resources'] = [{
            'url': DOWNLOAD_ENDPOINT_TEMPLATE.format(
                domain=urlparse(harvest_object.source.url).hostname,
                resource_id=res['resource']['id']),
            'format': 'CSV'
        }]

        return package_dict
    
    def _set_config(self, config_str):
        if config_str:
            self.config = self.validate_config(config_str)
            if 'base_api_endpoint' in self.config:
                self.base_api_endpoint = self.config['base_api_endpoint']

            log.debug('Using config: %r', self.config)
        else:
            self.config = {'base_api_endpoint':BASE_API_ENDPOINT}

    def validate_config(self, config):
        if not config:
            return config

        config_obj = json.loads(config)
        if 'base_api_endpoint' in config_obj:
            try:
                parsed = urlparse(config_obj['base_api_endpoint'])
            except AttributeError:
                raise ValueError('base_api_endpoint must be a valid URL')
            if not parsed.scheme or not parsed.netloc:
                raise ValueError('base_api_endpoint must be a valid URL')
        return config_obj
    
    def process_package(self, package, harvest_object):
        '''
        Subclasses can override this method to perform additional processing on
        package dicts during import_stage.
        '''
        return package

    def info(self):
        return {
            'name': 'socrata',
            'title': 'Socrata',
            'description': 'Harvests from Socrata data catalogues'
        }

    def gather_stage(self, harvest_job):
        '''
        Gather dataset content from Socrate and create HarvestObjects for each
        dataset.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''

        def _request_datasets_from_socrata(domain, limit=100, offset=0):
            api_request_url = \
                '{0}?domains={1}&search_context={1}' \
                '&only=datasets&limit={2}&offset={3}' \
                .format(self.base_api_endpoint, domain, limit, offset)
            log.debug('Requesting {}'.format(api_request_url))
            api_response = requests.get(api_request_url)

            try:
                api_json = api_response.json()
            except JSONDecodeError:
                self._save_gather_error(
                    'Gather error: Invalid response from {}'
                    .format(api_request_url),
                    harvest_job)
                return None

            if 'error' in api_json:
                self._save_gather_error('Gather error: {}'
                                        .format(api_json['error']),
                                        harvest_job)
                return None

            return api_json['results']

        def _page_datasets(domain, batch_number):
            '''Request datasets by page until an empty array is returned'''
            current_offset = 0
            while True:
                datasets = \
                    _request_datasets_from_socrata(domain, batch_number,
                                                   current_offset)
                if datasets is None or len(datasets) == 0:
                    break
                current_offset = current_offset + batch_number
                log.debug(f'Continued with {current_offset}-{batch_number}')
                for dataset in datasets:
                    yield dataset

        def _make_harvest_objs(datasets):
            '''Create HarvestObject with Socrata dataset content.'''
            obj_ids = []
            guids = []
            for d in datasets:
                log.debug('Creating HarvestObject for {} {}'
                          .format(d['resource']['name'],
                                  d['resource']['id']))
                obj = HarvestObject(guid=d['resource']['id'],
                                    job=harvest_job,
                                    content=json.dumps(d),
                                    extras=[HarvestObjectExtra(
                                                        key='status',
                                                        value='hi!')])
                obj.save()
                obj_ids.append(obj.id)
                guids.append(d['resource']['id'])
            return obj_ids, guids

        log.debug('In SocrataHarvester gather_stage (%s)',
                  harvest_job.source.url)
        self._set_config(harvest_job.source.config)
        domain = urlparse(harvest_job.source.url).hostname

        object_ids, guids = _make_harvest_objs(_page_datasets(domain, 100))

        # Check if some datasets need to be deleted
        object_ids_to_delete = \
            self._mark_datasets_for_deletion(guids, harvest_job)

        object_ids.extend(object_ids_to_delete)

        return object_ids

    def fetch_stage(self, harvest_object):
        '''
        No fetch required, all package data obtained from gather stage.
        '''
        return True

    def import_stage(self, harvest_object):
        '''

        '''
        log.debug('In SocrataHarvester import_stage')

        base_context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True
        }

        status = self._get_object_extra(harvest_object, 'status')
        if status == 'delete':
            # Delete package
            toolkit.get_action('package_delete')(
                base_context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'
                     .format(harvest_object.package_id, harvest_object.guid))
            return True

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(HarvestObject.current == True) \
            .first()  # noqa

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        # Check if a dataset with the same guid exists
        existing_dataset = self._get_existing_dataset(harvest_object.guid)

        # Delete package (dev testing)
        # if existing_dataset:
        #     self._delete_dataset(existing_dataset['id'])
        # return False

        package_dict = self._build_package_dict(base_context, harvest_object)

        self.process_package(package_dict, harvest_object)

        if existing_dataset:
            # Do we need to update?
            existing_updated_at = self._get_package_extra(existing_dataset,
                                                          'source_updated_at')
            source_updated_at = self._get_package_extra(package_dict,
                                                        'source_updated_at')
            if existing_updated_at and source_updated_at and \
               parse(existing_updated_at) == parse(source_updated_at):
                return 'unchanged'

            package_dict['id'] = existing_dataset['id']
            harvest_object.package_id = package_dict['id']
            harvest_object.add()
            try:
                toolkit.get_action('package_update')(
                    base_context.copy(),
                    package_dict
                )
            except Exception as e:
                self._save_object_error('Error updating package for {}: {}'
                                        .format(harvest_object.id, e),
                                        harvest_object, 'Import')
                return False

        else:
            # We need to explicitly provide a package ID
            package_dict['id'] = str(uuid.uuid4())

            harvest_object.package_id = package_dict['id']
            harvest_object.add()

            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute(
                'SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                toolkit.get_action('package_create')(
                    base_context.copy(),
                    package_dict
                )
            except Exception as e:
                self._save_object_error('Error creating package for {}: {}'
                                        .format(harvest_object.id, e),
                                        harvest_object, 'Import')
                return False

        return True
