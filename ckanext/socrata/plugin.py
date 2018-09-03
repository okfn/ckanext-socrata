from __future__ import unicode_literals

import json
from urlparse import urlparse

import requests
from simplejson.scanner import JSONDecodeError

from ckan import model
from ckan.lib.munge import munge_title_to_name, munge_tag
from ckan.plugins.core import implements
import ckan.plugins.toolkit as toolkit
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject

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
                .format(BASE_API_ENDPOINT, domain, limit, offset)
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
                if len(datasets) == 0:
                    raise StopIteration
                current_offset = current_offset + batch_number
                for dataset in datasets:
                    yield dataset

        def _make_harvest_objs(datasets):
            '''Create HarvestObject with Socrata dataset content.'''
            obj_ids = []
            for d in datasets:
                log.debug('Creating HarvestObject for {} {}'
                          .format(d['resource']['name'],
                                  d['resource']['id']))
                obj = HarvestObject(guid=d['resource']['id'],
                                    job=harvest_job,
                                    content=json.dumps(d))
                obj.save()
                obj_ids.append(obj.id)
            return obj_ids

        log.debug('In SocrataHarvester gather_stage (%s)',
                  harvest_job.source.url)

        domain = urlparse(harvest_job.source.url).hostname

        return _make_harvest_objs(_page_datasets(domain, 100))

    def fetch_stage(self, harvest_object):
        '''
        No fetch required, all package data obtained from gather stage.
        '''
        return True

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g.
              create, update or delete a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - setting the HarvestObject.package (if there is one)
            - setting the HarvestObject.current for this harvest:
               - True if successfully created/updated
               - False if successfully deleted
            - setting HarvestObject.current to False for previous harvest
              objects of this harvest source if the action was successful.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - creating the HarvestObject - Package relation (if necessary)
            - returning True if the action was done, "unchanged" if the object
              didn't need harvesting after all or False if there were errors.

        NB You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                  need harvesting after all or False if there were errors.
        '''
        log.debug('In SocrataHarvester import_stage')

        base_context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True
        }
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        # Local harvest source organization
        source_dataset = toolkit.get_action('package_show')(
            base_context.copy(),
            {'id': harvest_object.source.id}
        )
        local_org = source_dataset.get('owner_org')

        res = json.loads(harvest_object.content)
        package_dict = {
            'title': res['resource']['name'],
            'name': munge_title_to_name(res['resource']['name']),
            'url': res.get('permalink', ''),
            'notes': res['resource'].get('description', ''),
            'author': res['resource']['attribution'],
            'tags': [],
            'extras': [{
                'key': 'socrata_id', 'value': res['resource']['id']
            }],
            'owner_org': local_org,
            'resources': []
        }

        # Add tags
        package_dict['tags'] = \
            [{'name': munge_tag(t)}
             for t in res['classification'].get('tags', [])
             + res['classification'].get('domain_tags', [])]

        # Add domain_metadata to extras
        package_dict['extras'].extend(res['classification']
                                      .get('domain_metadata', []))

        # Resources
        package_dict['resources'] = [{
            'url': DOWNLOAD_ENDPOINT_TEMPLATE.format(
                domain=urlparse(harvest_object.source.url).hostname,
                resource_id=res['resource']['id']),
            'format': 'CSV'
        }]

        # log.debug(package_dict)

        try:
            create_response = toolkit.get_action('package_create')(
                base_context.copy(),
                package_dict
            )
            # self._create_or_update_package(package_dict, harvest_object,
            #                                package_dict_form='package_show')
        except Exception as e:
            self._save_object_error('Error creating package for {}: {}'
                                    .format(harvest_object.id, e),
                                    harvest_object, 'Import')
            raise e

        return True
