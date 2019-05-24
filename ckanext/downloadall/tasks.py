import tempfile
import zipfile
import os
import urlparse
import hashlib

import requests
import ckanapi
import ckanapi.datapackage

from ckan import model
from ckan.plugins.toolkit import get_action


log = __import__('logging').getLogger(__name__)


def update_zip(package_id):
    '''
    Create/update the a dataset's zip resource, containing the other resources
    and some metadata.
    '''
    # TODO deal with private datasets - 'ignore_auth': True
    context = {'model': model, 'session': model.Session}
    dataset = get_action('package_show')(context, {'id': package_id})
    log.debug('Updating zip {}'.format(dataset['name']))

    # 'filename' = "{0}.zip".format(dataset['name'])
    with tempfile.NamedTemporaryFile() as fp:
        existing_zip_resource, filesize = write_zip(fp, package_id)

        # Upload resource to CKAN as a new/updated resource
        registry = ckanapi.LocalCKAN()
        fp.seek(0)
        resource = dict(
            package_id=dataset['id'],
            url='dummy-value',
            upload=fp,
            name=u'All resource data',
            downloadall_metadata_modified=dataset['metadata_modified']
        )

        if not existing_zip_resource:
            log.debug('Writing new zip resource - {}'.format(dataset['name']))
            registry.action.resource_create(**resource)
        else:
            # TODO update the existing zip resource (using patch?)
            log.debug('Updating zip resource - {}'.format(dataset['name']))
            registry.action.resource_patch(
                id=existing_zip_resource['id'],
                **resource)


def write_zip(fp, package_id):
    '''
    Downloads resources and writes the zip file.

    :param fp: Open file that the zip can be written to
    '''
    resource_formats_to_ignore = ['API', 'api']  # TODO make it configurable

    with zipfile.ZipFile(fp, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) \
            as zipf:
        context = {'model': model, 'session': model.Session}
        dataset = get_action('package_show')(
            context, {'id': package_id})

        # filter out resources that are not suitable for inclusion in the data
        # package
        existing_zip_resource = None
        ckan = ckanapi.LocalCKAN()
        resources_to_include = []
        for i, res in enumerate(dataset['resources']):
            if res.get('downloadall_metadata_modified'):
                # this is an existing zip of all the other resources
                log.debug('Resource resource {}/{} skipped - is the zip itself'
                          .format(i + 1, len(dataset['resources'])))
                existing_zip_resource = res
                continue

            if res['format'] in resource_formats_to_ignore:
                log.debug('Resource resource {}/{} skipped - because it is '
                          'format {}'.format(i + 1, len(dataset['resources']),
                          res['format']))
                continue
            resources_to_include.append(res)
        dataset = dict(dataset, resources=resources_to_include)

        # get the datapackage (metadata)
        datapackage = ckanapi.datapackage.dataset_to_datapackage(dataset)

        i = 0
        for res, dres in zip(resources_to_include,
                             datapackage.get('resources', [])):
            i += 1

            ckanapi.datapackage.populate_datastore_res_fields(
                ckan=ckan, res=res)

            # TODO deal with a resource of resource_type=file.upload

            log.debug('Downloading resource {}/{}: {}'
                      .format(i + 1, len(dataset['resources']), res['url']))
            try:
                r = requests.get(res['url'], stream=True)
            except requests.ConnectionError:
                log.error('URL {url} refused connection. The resource will not'
                          ' be downloaded'.format(url=res['url']))
            except requests.exceptions.RequestException as e:
                log.error('URL {url} download request exception: {error}'
                          .format(url=res['url'], error=str(e)))
            except Exception as e:
                log.error('URL {url} download exception: {error}'
                          .format(url=res['url'], error=str(e)))

            filename = \
                ckanapi.datapackage.resource_filename(dres)

            hash_object = hashlib.md5()
            try:
                # python3 syntax - stream straight into the zip
                with zipf.open(filename, 'wb') as zf:
                    for chunk in r.iter_content(chunk_size=128):
                        zf.write(chunk)
                        hash_object.update(chunk)
            except RuntimeError:
                # python2 syntax - need to save to disk first
                with tempfile.NamedTemporaryFile() as datafile:
                    for chunk in r.iter_content(chunk_size=128):
                        datafile.write(chunk)
                        hash_object.update(chunk)
                    datafile.flush()
                    # .write() streams the file into the zip
                    zipf.write(datafile.name, arcname=filename)
            file_hash = hash_object.hexdigest()
            # TODO optimize using the file_hash
            file_hash
        # TODO deal with a dataset with no resources

        # Add the datapackage.json
        with tempfile.NamedTemporaryFile() as json_file:
            json_file.write(ckanapi.cli.utils.pretty_json(datapackage))
            json_file.flush()
            zipf.write(json_file.name, arcname='datapackage.json')

        # write HTML index
        # env = jinja2.Environment(loader=jinja2.PackageLoader(
        #     'ckanext.downloadll', 'templates'))
        # env.filters['datetimeformat'] = datetimeformat
        # template = env.get_template('index.html')
        # zipf.writestr('index.html',
        #     template.render(datapackage=datapackage,
        #                     date=datetime.datetime.now()).encode('utf8'))

        # Strip out unnecessary data from datapackage
        # for res in datapackage['resources']:
        #     del res['has_data']
        #     if 'cache_filepath' in res:
        #         del res['cache_filepath']
        #     if 'reason' in res:
        #         del res['reason']
        #     if 'detected_format' in res:
        #         del res['detected_format']

        # zipf.writestr('datapackage.json',
        #               json.dumps(datapackage, indent=4))

    statinfo = os.stat(fp.name)
    filesize = statinfo.st_size

    log.info('Zip created: {} {} bytes'.format(fp.name, filesize))

    return existing_zip_resource, filesize
