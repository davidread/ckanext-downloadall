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

    print('Updating zip')
    # get dataset
    # TODO deal with private datasets - 'ignore_auth': True
    context = {'model': model, 'session': model.Session}
    dataset = get_action('package_show')(context, {'id': package_id})

    # 'filename' = "{0}.zip".format(dataset['name'])
    with tempfile.NamedTemporaryFile() as fp:
        with zipfile.ZipFile(fp, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) \
                as zipf:
            try:
                dataset = get_action('package_show')(
                    context, {'id': package_id})
            except KeyError as exc:
                log.error('Cannot find action - check this plugin is enabled: '
                          '{}'.format(exc))
                raise

            # download all the data and write it to the zip
            downloadall_resource = None
            for res in dataset['resources']:
                if res.get('downloadall_metadata_modified'):
                    # this is an existing zip of all the other resources
                    downloadall_resource = res
                    return

                # TODO deal with a resource of resource_type=file.upload

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

        # Upload resource to CKAN
        # upload: FieldStorage (optional) needs multipart/form-data
        class FakeFileStorage(cgi.FieldStorage):
            def __init__(self, fp, filename):
                self.file = fp
                self.filename = filename
                self.name = 'upload'
        payload = FakeFileStorage(fp, fp.name)
        resource = {
            'package_id': dataset['id'],
            # 'url': 'http://data',
            'name': 'All resource data',
            'upload': payload,
            'downloadall_metadata_modified': dataset['metadata_modified'],
        }

        context = {'model': model, 'ignore_auth': True,
                   'user': 'ckanext-downloadall', 'session': model.Session}
        if not downloadall_resource:
            get_action('resource_create')(context, resource)
        else:
            # TODO update the existing zip resource (using patch?)
            downloadall_resource

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
