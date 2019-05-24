import __builtin__ as builtins
import zipfile
import json

import mock
from nose.tools import assert_equal
from pyfakefs import fake_filesystem
import responses

from ckan.tests import factories, helpers
import ckan.lib.uploader
from ckanext.downloadall.tasks import update_zip


# Uploads are put in this fake file system
# Copied from ckan/tests/logic/action/test_create.py
real_open = open
fs = fake_filesystem.FakeFilesystem()
fake_os = fake_filesystem.FakeOsModule(fs)
fake_open = fake_filesystem.FakeFileOpen(fs)

eq = assert_equal

def mock_open_if_open_fails(*args, **kwargs):
    try:
        return real_open(*args, **kwargs)
    except (OSError, IOError):
        return fake_open(*args, **kwargs)


@mock.patch.object(ckan.lib.uploader, 'os', fake_os)
@mock.patch.object(builtins, 'open', side_effect=mock_open_if_open_fails)
@mock.patch.object(ckan.lib.uploader, '_storage_path', new='/doesnt_exist')
class TestUpdateZip(object):
    @classmethod
    def setupClass(cls):
        helpers.reset_db()

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_simple(self, _):
        responses.add(
            responses.GET,
            'https://example.com/data.csv',
            body='a,b,c'
        )
        responses.add_passthru('http://127.0.0.1:8983/solr')
        dataset = factories.Dataset(resources=[{
            'url': 'https://example.com/data.csv',
            'format': 'csv',
            }])

        update_zip(dataset['id'])

        dataset = helpers.call_action(u'package_show', id=dataset['id'])
        zip_resources = [res for res in dataset['resources']
                         if res['name'] == u'All resource data']
        assert_equal(len(zip_resources), 1)
        zip_resource = zip_resources[0]
        assert_equal(zip_resource['url_type'], 'upload')

        uploader = ckan.lib.uploader.get_resource_uploader(zip_resource)
        filepath = uploader.get_path(zip_resource[u'id'])
        csv_filename_in_zip = '{}.csv'.format(dataset['resources'][0]['id'])
        with fake_open(filepath, 'rb') as f:
            with zipfile.ZipFile(f) as zip_:
                assert_equal(zip_.namelist(),
                             [csv_filename_in_zip, 'datapackage.json'])
                assert_equal(zip_.read(csv_filename_in_zip), 'a,b,c')
                datapackage_json = zip_.read('datapackage.json')
                assert datapackage_json.startswith('{\n  "description"')
                datapackage = json.loads(datapackage_json)
                eq(datapackage[u'name'][:12], u'test_dataset')
                eq(datapackage[u'title'], u'Test Dataset')
                eq(datapackage[u'description'], u'Just another test dataset.')
                eq(datapackage[u'resources'], [{
                    u'format': u'CSV',
                    u'name': dataset['resources'][0]['id'],
                    u'path': u'https://example.com/data.csv'}])



    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_update_twice(self, _):
        responses.add(
            responses.GET,
            'https://example.com/data.csv',
            body='a,b,c'
        )
        responses.add_passthru('http://127.0.0.1:8983/solr')
        dataset = factories.Dataset(resources=[{
            'url': 'https://example.com/data.csv',
            'format': 'csv',
            }])

        update_zip(dataset['id'])
        update_zip(dataset['id'])

        # ensure the zip isn't included in the zip the second time
        dataset = helpers.call_action(u'package_show', id=dataset['id'])
        zip_resources = [res for res in dataset['resources']
                         if res['name'] == u'All resource data']
        assert_equal(len(zip_resources), 1)
