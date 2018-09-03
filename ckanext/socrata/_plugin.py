import logging
import urllib2
from itertools import count
from datetime import datetime

from ckan.logic.action.create import package_create_rest
from ckan.logic.action.update import package_update_rest
from ckan.logic.action.get import package_show
from ckan.logic.schema import default_package_schema
from ckan.logic import ValidationError, NotFound
from ckan import model
from ckan.model import Session, Package
from ckan.lib.navl.validators import ignore_missing
from ckan.lib.munge import munge_title_to_name
from ckan.lib.helpers import json

from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, \
                                  HarvestGatherError, HarvestObjectError

log = logging.getLogger(__name__)

TABLE_REPRESENTATIONS = [
    ('csv', 'text/csv', 'Comma-separated values'),
    ('json', 'application/json', 'JSON objects'),
    ('pdf', 'application/pdf', 'PDF Print view'),
    ('rdf', 'application/xml+rdf', 'RDF Triples'),
    ('rss', 'application/xml+rss', 'RSS Newsfeed'),
    ('xls', 'application/vnd.ms-excel', 'MS Excel file'),
    ('xlsx',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'MS Excel 2007 file'),
    ('xml', 'application/xml', 'XML Document')]


class SocrataHarvester(SingletonPlugin):

    implements(IHarvester)

    def _gen_new_name(self, title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        like_q = u'%s%%' % name
        pkg_query = \
            Session.query(Package).filter(Package.name.ilike(like_q)) \
            .limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

    def _save_gather_error(self, message, job):
        err = HarvestGatherError(message=message, job=job)
        err.save()
        log.error(message)

    def _save_object_error(self, message, obj, stage=u'Fetch'):
        err = HarvestObjectError(message=message, object=obj, stage=stage)
        err.save()
        log.error(message)

    def _create_or_update_package(self, package_dict, harvest_object):
        '''
            Creates a new package or updates an exisiting one according to the
            package dictionary provided. The package dictionary should look
            like the REST API response for a package:

            http://ckan.net/api/rest/package/statistics-catalunya

            Note that the package_dict must contain an id, which will be used
            to check if the package needs to be created or updated (use the
            remote dataset id).

            If the remote server provides the modification date of the remote
            package, add it to package_dict['metadata_modified'].

        '''
        try:
            # from pprint import pprint
            # pprint(package_dict)
            # change default schema
            schema = default_package_schema()
            schema["id"] = [ignore_missing, unicode]

            context = {
                'model': model,
                'session': Session,
                'user': u'harvest',
                'api_version': '2',
                'schema': schema,
            }

            # Check if package exists
            context.update({'id': package_dict['id']})
            try:
                existing_package_dict = package_show(context)
                # Check modified date
                if 'metadata_modified' not in package_dict or \
                   package_dict['metadata_modified'] > \
                   existing_package_dict['metadata_modified']:
                    log.info('Package with GUID %s exists and needs to be updated' % harvest_object.guid)
                    # Update package
                    updated_package = \
                        package_update_rest(package_dict, context)

                    harvest_object.package_id = updated_package['id']
                    harvest_object.save()
                else:
                    log.info('Package with GUID %s not updated, skipping...' %
                             harvest_object.guid)

            except NotFound:
                # Package needs to be created
                del context['id']
                log.info('Package with GUID %s does not exist, let\'s create it' % harvest_object.guid)
                new_package = package_create_rest(package_dict, context)
                harvest_object.package_id = new_package['id']
                harvest_object.save()

            return True

        except ValidationError as e:
            log.exception(e)
            self._save_object_error('Invalid package with GUID %s: %r' %
                                    (harvest_object.guid, e.error_dict),
                                    harvest_object, 'Import')
        except Exception as e:
            log.exception(e)
            self._save_object_error('%r' % e, harvest_object, 'Import')

    def info(self):
        return {
            'name': 'socrata',
            'title': 'Socrata',
            'description': 'Data catalogue import'
        }

    def gather_stage(self, harvest_job):
        log.debug('In SocrataHarvester gather_stage')
        base_url = harvest_job.source.url.strip("/")
        limit = 20
        ids = []
        for page in count():
            url = \
                base_url + "/api/search/views.json?q=&limit=%s&page=%s" % \
                (limit, page+1)
            print "URL", url
            indexfh = urllib2.urlopen(url)
            result = json.load(indexfh)[0]
            indexfh.close()
            for res in result.get('results', []):
                id = res.get('id')
                obj = HarvestObject(guid=id, job=harvest_job,
                                    content=json.dumps(res))
                obj.save()
                ids.append(obj.id)
            break
            if (page+1)*limit > result.get('count'):
                break
        return ids

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        try:
            res = json.loads(harvest_object.content)
            package_dict = {
                    'title': res['name'],
                    'url': res.get('attributionLink', ''),
                    'notes': res.get('description', ''),
                    'author': res['owner']['displayName'],
                    'maintainer': res['tableAuthor']['displayName'],
                    'tags': res.get('tags', []),
                    'extras': {
                        'date_released':
                            datetime.fromtimestamp(res['rowsUpdatedAt'])
                            .isoformat(),
                        'categories': res.get('category', ''),
                        'license_summary': res.get('licenseId', ''),
                        'socrata_id': res['id']
                    },
                    'resources': []
                }
            if res.get('displayType') == 'table':
                url = harvest_object.job.source.url.strip('/')
                url += '/views/%s/rows.%s?accessType=DOWNLOAD'
                for fmt, mime, name in TABLE_REPRESENTATIONS:
                    r = {
                            'url': url % (res['id'], fmt),
                            'format': mime,
                            'description': name
                        }
                    package_dict['resources'].append(r)

        except Exception, e:
            log.exception(e)
            self._save_object_error('%r' % e, harvest_object, 'Import')

        package_dict['id'] = harvest_object.guid
        package_dict['name'] = self._gen_new_name(package_dict['title'])
        return self._create_or_update_package(package_dict, harvest_object)
