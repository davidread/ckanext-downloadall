"""Tests for plugin.py."""
from nose.tools import assert_equal

from ckan.tests import factories
from ckan.tests import helpers
from ckan import plugins as p


class TestDatastoreCreate(object):
    @classmethod
    def setupClass(cls):
        p.load(u'downloadall')
        p.load(u'datastore')
        helpers.reset_db()

    def setup(self):
        helpers.call_action(u'job_clear')

    @classmethod
    def teardown_class(cls):
        p.unload(u'downloadall')
        p.unload(u'datastore')

    def test_datastore_create(self):
        dataset = factories.Dataset(resources=[
            {'url': 'http://some.image.png', 'format': 'png'}])
        helpers.call_action(u'job_clear')

        helpers.call_action(u'datastore_create',
                            resource_id=dataset['resources'][0]['id'],
                            force=True)

        # Check the chained action caused the zip to be queued for update
        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll datastore_create "{}" {}'
             .format(dataset['name'], dataset['id'])])
