import __builtin__ as builtins
import zipfile
import json
import tempfile
import re

import mock
from nose.tools import assert_equal
from pyfakefs import fake_filesystem
import responses
import requests

from ckan.tests import factories, helpers
import ckan.lib.uploader
from ckanext.downloadall.tasks import update_zip
import ckanapi


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
                    u'path': csv_filename_in_zip,
                    u'sources': [{u'path': u'https://example.com/data.csv',
                                  u'title': None}],
                    }])

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

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_uploaded_resource(self, _):
        responses.add_passthru('http://127.0.0.1:8983/solr')
        csv_content = u'Test,csv'
        responses.add(
            responses.GET,
            re.compile(r'http://test.ckan.net/dataset/.*/download/.*'),
            body=csv_content
        )
        dataset = factories.Dataset()
        # add a resource which is an uploaded file
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(csv_content)
            fp.seek(0)
            registry = ckanapi.LocalCKAN()
            resource = dict(
                package_id=dataset[u'id'],
                url=u'dummy-value',
                upload=fp,
                name=u'Rainfall',
                format=u'CSV'
            )
            registry.action.resource_create(**resource)

        update_zip(dataset['id'])

        dataset = helpers.call_action(u'package_show', id=dataset['id'])
        zip_resources = [res for res in dataset['resources']
                         if res['name'] == u'All resource data']
        zip_resource = zip_resources[0]
        uploader = ckan.lib.uploader.get_resource_uploader(zip_resource)
        filepath = uploader.get_path(zip_resource[u'id'])
        csv_filename_in_zip = u'rainfall.csv'
        with fake_open(filepath, 'rb') as f:
            with zipfile.ZipFile(f) as zip_:
                # Check uploaded file
                assert_equal(zip_.namelist(),
                             [csv_filename_in_zip, 'datapackage.json'])
                assert_equal(zip_.read(csv_filename_in_zip), 'Test,csv')
                # Check datapackage.json
                datapackage_json = zip_.read('datapackage.json')
                datapackage = json.loads(datapackage_json)
                eq(datapackage[u'resources'], [{
                    u'format': u'CSV',
                    u'name': u'rainfall',
                    u'path': csv_filename_in_zip,
                    u'sources': [{u'path': dataset['resources'][0]['url'],
                                  u'title': u'Rainfall'}],
                    u'title': u'Rainfall',
                    }])

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_resource_url_with_connection_error(self, _):
        responses.add_passthru('http://127.0.0.1:8983/solr')
        responses.add(
            responses.GET,
            'https://example.com/data.csv',
            body=requests.ConnectionError('Some network trouble...')
        )
        dataset = factories.Dataset(resources=[{
            'url': 'https://example.com/data.csv',
            'name': 'rainfall',
            'format': 'csv',
            }])

        update_zip(dataset['id'])

        dataset = helpers.call_action(u'package_show', id=dataset['id'])
        zip_resources = [res for res in dataset['resources']
                         if res['name'] == u'All resource data']
        zip_resource = zip_resources[0]
        uploader = ckan.lib.uploader.get_resource_uploader(zip_resource)
        filepath = uploader.get_path(zip_resource[u'id'])
        with fake_open(filepath, 'rb') as f:
            with zipfile.ZipFile(f) as zip_:
                # Zip doesn't contain the data, just the json file
                assert_equal(zip_.namelist(),
                             ['datapackage.json'])
                # Check datapackage.json
                datapackage_json = zip_.read('datapackage.json')
                datapackage = json.loads(datapackage_json)
                eq(datapackage[u'resources'], [{
                    u'format': u'CSV',
                    u'name': u'rainfall',
                    # path is to the URL - an 'external resource'
                    u'path': 'https://example.com/data.csv',
                    u'title': u'rainfall',
                    }])

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_resource_url_with_404_error(self, _):
        responses.add_passthru('http://127.0.0.1:8983/solr')
        responses.add(
            responses.GET,
            'https://example.com/data.csv',
            status=404
        )
        dataset = factories.Dataset(resources=[{
            'url': 'https://example.com/data.csv',
            'name': 'rainfall',
            'format': 'csv',
            }])

        update_zip(dataset['id'])

        dataset = helpers.call_action(u'package_show', id=dataset['id'])
        zip_resources = [res for res in dataset['resources']
                         if res['name'] == u'All resource data']
        zip_resource = zip_resources[0]
        uploader = ckan.lib.uploader.get_resource_uploader(zip_resource)
        filepath = uploader.get_path(zip_resource[u'id'])
        with fake_open(filepath, 'rb') as f:
            with zipfile.ZipFile(f) as zip_:
                # Zip doesn't contain the data, just the json file
                assert_equal(zip_.namelist(),
                             ['datapackage.json'])
                # Check datapackage.json
                datapackage_json = zip_.read('datapackage.json')
                datapackage = json.loads(datapackage_json)
                eq(datapackage[u'resources'], [{
                    u'format': u'CSV',
                    u'name': u'rainfall',
                    # path is to the URL - an 'external resource'
                    u'path': 'https://example.com/data.csv',
                    u'title': u'rainfall',
                    }])
