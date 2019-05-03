import __builtin__ as builtins
import zipfile

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


def mock_open_if_open_fails(*args, **kwargs):
    try:
        return real_open(*args, **kwargs)
    except (OSError, IOError):
        return fake_open(*args, **kwargs)


class TestUpdateZip(object):
    @classmethod
    def setupClass(cls):
        helpers.reset_db()

    @responses.activate
    @helpers.change_config('ckan.storage_path', '/doesnt_exist')
    @mock.patch.object(ckan.lib.uploader, 'os', fake_os)
    @mock.patch.object(builtins, 'open', side_effect=mock_open_if_open_fails)
    @mock.patch.object(ckan.lib.uploader, '_storage_path', new='/doesnt_exist')
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
        with fake_open(filepath, 'rb') as f:
            with zipfile.ZipFile(f) as zip_:
                assert_equal(zip_.namelist(), ['data.csv'])
                assert_equal(zip_.read('data.csv'), 'a,b,c')
