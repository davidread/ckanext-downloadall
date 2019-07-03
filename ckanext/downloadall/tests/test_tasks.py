import __builtin__ as builtins
import zipfile
import json
import tempfile
import re
import copy

import mock
from nose.tools import assert_equal
from pyfakefs import fake_filesystem
import responses
import requests

from ckan.tests import factories, helpers
import ckan.lib.uploader
from ckanext.downloadall.tasks import (
    update_zip, canonized_datapackage, save_local_path_in_datapackage_resource,
    hash_datapackage, generate_datapackage_json)
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


def mock_populate_datastore_res_fields(ckan, res):
    res['datastore_fields'] = [{u'type': u'int', u'id': u'_id'},
                               {u'type': u'text', u'id': u'Date'},
                               {u'type': u'text', u'id': u'Price'}]


def mock_populate_datastore_res_fields_overridden(ckan, res):
    res['datastore_fields'] = [
        {u'type': u'int', u'id': u'_id'},
        {
            u'type': u'timestamp',
            u'id': u'Date',
            u'info': {
                u'notes': u'Some description here!',
                u'type_override': u'timestamp',
                u'label': u'The Date'
            },
        },
        {
            u'type': u'numeric',
            u'id': u'Price',
            u'info': {u'notes': u'', u'type_override': u'', u'label': u''},
        }
    ]


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
        update_zip(dataset['id'], skip_if_no_changes=False)

        # ensure a second zip hasn't been added
        dataset = helpers.call_action(u'package_show', id=dataset['id'])
        zip_resources = [res for res in dataset['resources']
                         if res['name'] == u'All resource data']
        assert_equal(len(zip_resources), 1)

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_dont_skip_if_no_changes(self, _):
        # i.e. testing skip_if_no_changes=False
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
        with mock.patch('ckanext.downloadall.tasks.write_zip') as write_zip_:
            update_zip(dataset['id'], skip_if_no_changes=False)
            # ensure zip would be rewritten in this case - not letting it skip
            assert write_zip_.called

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_update_twice_skipping_second_time(self, _):
        # i.e. testing skip_if_no_changes=False
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
        with mock.patch('ckanext.downloadall.tasks.write_zip') as write_zip_:
            update_zip(dataset['id'], skip_if_no_changes=True)
            # nothings changed, so it shouldn't rewrite the zip
            assert not write_zip_.called

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_changing_description_causes_zip_to_update(self, _):
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
        dataset = helpers.call_action(u'package_patch', id=dataset['id'],
                                      notes='New notes')
        with mock.patch('ckanext.downloadall.tasks.write_zip') as write_zip_:
            update_zip(dataset['id'], skip_if_no_changes=True)
            # ensure zip would be rewritten in this case - not letting it skip
            assert write_zip_.called

    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_deleting_resource_causes_zip_to_update(self, _):
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
        dataset = helpers.call_action(u'package_patch', id=dataset['id'],
                                      resources=[])
        with mock.patch('ckanext.downloadall.tasks.write_zip') as write_zip_:
            update_zip(dataset['id'], skip_if_no_changes=True)
            # ensure zip would be rewritten in this case - not letting it skip
            assert write_zip_.called

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

    @mock.patch('ckanapi.datapackage.populate_datastore_res_fields',
                side_effect=mock_populate_datastore_res_fields)
    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @responses.activate
    def test_data_dictionary(self, _, __):
        responses.add(
            responses.GET,
            'https://example.com/data.csv',
            body='Date,Price\n1/6/2017,4.00\n2/6/2017,4.12'
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
                datapackage_json = zip_.read('datapackage.json')
                assert datapackage_json.startswith('{\n  "description"')
                datapackage = json.loads(datapackage_json)
                eq(datapackage['resources'][0][u'schema'],
                   {'fields': [{'type': 'string', 'name': u'Date'},
                               {'type': 'string', 'name': u'Price'}]})

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


local_datapackage = {
    "license": {
        "title": "Creative Commons Attribution",
        "type": "cc-by",
        "url": "http://www.opendefinition.org/licenses/cc-by"
    },
    "name": "test",
    "resources": [
        {
            "format": "CSV",
            "name": "annual-csv",
            "path": "annual-.csv",
            "schema": {
                "fields": [
                    {
                        "description": "Some description here!",
                        "name": "Date",
                        "title": "The Date",
                        "type": "datetime"
                    },
                    {
                        "name": "Price",
                        "type": "number"
                    }
                ]
            },
            "sources": [
                {
                    "path": "https://sample.com/annual.csv",
                    "title": "annual.csv"
                }
            ],
            "title": "annual.csv"
        },
        {
            "format": "CSV",
            "name": "annual-csv0",
            "path": "annual-csv0.csv",
            "schema": {
                "fields": [
                    {
                        "name": "Date",
                        "type": "string"
                    },
                    {
                        "name": "Price",
                        "type": "string"
                    }
                ]
            },
            "sources": [
                {
                    "path": "https://sample.com/annual.csv",
                    "title": "annual.csv"
                }
            ],
            "title": "annual.csv"
        }
    ],
    "title": "Gold Prices"
}
remote_datapackage = {
    "license": {
        "title": "Creative Commons Attribution",
        "type": "cc-by",
        "url": "http://www.opendefinition.org/licenses/cc-by"
    },
    "name": "test",
    "resources": [
        {
            "format": "CSV",
            "name": "annual-csv",
            "path": "https://sample.com/annual.csv",
            "schema": {
                "fields": [
                    {
                        "description": "Some description here!",
                        "name": "Date",
                        "title": "The Date",
                        "type": "datetime"
                    },
                    {
                        "name": "Price",
                        "type": "number"
                    }
                ]
            },
            "title": "annual.csv"
        },
        {
            "format": "CSV",
            "name": "annual-csv0",
            "path": "https://sample.com/annual.csv",
            "schema": {
                "fields": [
                    {
                        "name": "Date",
                        "type": "string"
                    },
                    {
                        "name": "Price",
                        "type": "string"
                    }
                ]
            },
            "title": "annual.csv"
        }
    ],
    "title": "Gold Prices"
}


class TestCanonizedDataPackage(object):
    def test_canonize_local_datapackage(self):
        eq(canonized_datapackage(local_datapackage), remote_datapackage)

    def test_canonize_remote_datapackage(self):
        eq(canonized_datapackage(remote_datapackage), remote_datapackage)


class TestSaveLocalPathInDatapackageResource(object):
    def test_convert_remote_to_local(self):
        datapackage = copy.deepcopy(remote_datapackage)
        res = {'title': 'Gold Price Annual'}
        save_local_path_in_datapackage_resource(
            datapackage['resources'][0], res, 'annual-.csv')
        save_local_path_in_datapackage_resource(
            datapackage['resources'][1], res, 'annual-csv0.csv')
        eq(datapackage, local_datapackage)


class TestHashDataPackage(object):
    def test_repeatability(self):
        # value of the hash shouldn't change between machines or python
        # versions etc
        eq(hash_datapackage({'resources': []}),
           '60482792d5032e490cdde4f759e84fd6')

    def test_dict_ordering(self):
        eq(hash_datapackage({'resources': [{'format': u'CSV', 'name': u'a'}]}),
           hash_datapackage({'resources': [{'name': u'a', 'format': u'CSV'}]}))


class TestGenerateDatapackageJson(object):
    @classmethod
    def setupClass(cls):
        helpers.reset_db()

    def test_simple(self):
        dataset = factories.Dataset(resources=[{
            'url': 'https://example.com/data.csv',
            'format': 'csv',
            }])

        datapackage, ckan_and_datapackage_resources, existing_zip_resource = \
            generate_datapackage_json(dataset['id'])

        replace_number_suffix(datapackage, 'name')
        replace_uuid(datapackage['resources'][0], 'name')
        eq(datapackage, {
            'description': u'Just another test dataset.',
            'name': u'test_dataset_num',
            'resources': [{'format': u'CSV',
                           'name': u'<SOME-UUID>',
                           'path': u'https://example.com/data.csv'}],
            'title': u'Test Dataset'
            })
        eq(ckan_and_datapackage_resources[0][0][u'url'],
           u'https://example.com/data.csv')
        eq(ckan_and_datapackage_resources[0][0][u'description'],
           u'')
        eq(ckan_and_datapackage_resources[0][1], {
            'format': u'CSV',
            'name': u'<SOME-UUID>',
            'path': u'https://example.com/data.csv'
        })
        eq(existing_zip_resource, None)

    def test_extras(self):
        dataset = factories.Dataset(extras=[
            {u'key': u'extra1', u'value': u'1'},
            {u'key': u'extra2', u'value': u'2'},
            {u'key': u'extra3', u'value': u'3'},
        ])

        datapackage, _, __ = \
            generate_datapackage_json(dataset['id'])

        replace_number_suffix(datapackage, 'name')
        eq(datapackage, {
            'description': u'Just another test dataset.',
            'name': u'test_dataset_num',
            'title': u'Test Dataset',
            'extras': {u'extra1': 1, u'extra2': 2, u'extra3': 3},
            })

    @helpers.change_config(
        'ckanext.downloadall.dataset_fields_to_add_to_datapackage',
        'num_resources type')
    def test_added_fields(self):
        dataset = factories.Dataset()

        datapackage, _, __ = \
            generate_datapackage_json(dataset['id'])

        replace_number_suffix(datapackage, 'name')
        eq(datapackage, {
            'description': u'Just another test dataset.',
            'name': u'test_dataset_num',
            'title': u'Test Dataset',
            'num_resources': 0,
            'type': u'dataset',
            })


# helpers

def zip_filepath(dataset):
    dataset = helpers.call_action(u'package_show',
                                  id=dataset['id'])
    zip_resources = [res for res in dataset['resources']
                     if res['name'] == u'All resource data']
    zip_resource = zip_resources[0]
    uploader = ckan.lib.uploader.get_resource_uploader(zip_resource)
    return uploader.get_path(zip_resource[u'id'])


class DataPackageZip(object):
    '''Opens the zipfile for the given dataset, so you can test its contents'''
    def __init__(self, dataset):
        self.dataset = dataset

    def __enter__(self):
        filepath = zip_filepath(self.dataset)
        self.f = open(filepath, 'rb')
        self.zip = zipfile.ZipFile(self.f)
        return self.zip

    def __exit__(self, ext, exv, trb):
        self.zip.close()
        self.f.close()


def extract_datapackage_json(dataset):
    with DataPackageZip(dataset) as zip_:
        assert 'datapackage.json' in zip_.namelist()
        datapackage_json = zip_.read('datapackage.json')
        datapackage = json.loads(datapackage_json)
        return datapackage


def replace_uuid(dict_, key):
    assert key in dict_
    dict_[key] = u'<SOME-UUID>'


def replace_datetime(dict_, key):
    assert key in dict_
    dict_[key] = u'2019-05-24T15:52:30.123456'


def replace_number_suffix(dict_, key):
    # e.g. "Test Dataset 23" -> "Test Dataset "
    assert key in dict_
    dict_[key] = re.sub(r'\d+$', 'num', dict_[key])
