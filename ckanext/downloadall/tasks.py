import tempfile
import zipfile
import os
import cgi
import urlparse
import hashlib

import requests

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
        # upload: FieldStorage (optional) needs multipart/form-data
        fp.seek(0)
        payload = cgi.FieldStorage()
        payload.file = fp
        payload.filename = fp.name
        resource = {
            'package_id': dataset['id'],
            # 'url': 'http://data',
            'name': 'All resource data',
            'upload': payload,
            'size': filesize,
            'downloadall_metadata_modified': dataset['metadata_modified'],
        }

        context = {'model': model, 'ignore_auth': True,
                   'user': 'ckanext-downloadall', 'session': model.Session}
        if not existing_zip_resource:
            log.debug('Writing new zip resource - {}'.format(dataset['name']))
            get_action('resource_create')(context, resource)
        else:
            # TODO update the existing zip resource (using patch?)
            resource['id'] = existing_zip_resource['id']
            log.debug('Updating zip resource - {}'.format(dataset['name']))
            get_action('resource_patch')(context, resource)

        # package_zip = PackageZip.get_for_package(package_id)
        # if not package_zip:
        #     PackageZip.create(package_id, filepath, filesize,
        #                       has_data=any_have_data)
        #     log.info('Package zip created: %s', filepath)
        # else:
        #     package_zip.filepath = filepath
        #     package_zip.updated = datetime.datetime.now()
        #     package_zip.size = filesize
        #     package_zip.has_data = any_have_data
        #     log.info('Package zip updated: %s', filepath)

        #     model.Session.add(package_zip)
        #     model.Session.commit()


def write_zip(fp, package_id):
    '''
    Downloads resources and writes the zip file.

    :param fp: Open file that the zip can be written to
    '''
    with zipfile.ZipFile(fp, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) \
            as zipf:
        context = {'model': model, 'session': model.Session}
        dataset = get_action('package_show')(
            context, {'id': package_id})

        # download all the data and write it to the zip
        existing_zip_resource = None
        for i, res in enumerate(dataset['resources']):
            if res.get('downloadall_metadata_modified'):
                # this is an existing zip of all the other resources
                log.debug('Resource resource {}/{} skipped - is the zip itself'
                          .format(i + 1, len(dataset['resources'])))
                existing_zip_resource = res
                continue

            # TODO deal with a resource of resource_type=file.upload

            log.debug('Downloading resource {}/{}: {}'
                      .format(i + 1, len(dataset['resources']), res['url']))
            r = requests.get(res['url'], stream=True)
            filename = os.path.basename(urlparse.urlparse(res['url']).path)
            # TODO deal with duplicate filenames in the zip
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

        # TODO add the datapackage.json

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
