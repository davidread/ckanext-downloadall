"""Tests for plugin.py."""
from nose.tools import assert_equal

from ckan.tests import factories
from ckan.tests import helpers
from ckan import plugins as p


class TestNotify(object):
    @classmethod
    def setupClass(cls):
        p.load(u'downloadall')
        helpers.reset_db()

    def setup(self):
        helpers.call_action(u'job_clear')

    @classmethod
    def teardown_class(cls):
        p.unload(u'downloadall')

    def test_new_resource_leads_to_queued_task(self):
        dataset = factories.Dataset(resources=[
            {'url': 'http://some.image.png', 'format': 'png'}])
        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll new "{}" {}'
             .format(dataset['name'], dataset['id'])])

    def test_changed_resource_leads_to_queued_task(self):
        dataset = factories.Dataset(resources=[
            {'url': 'http://some.image.png', 'format': 'png'}])
        helpers.call_action(u'job_clear')

        dataset['resources'][0]['url'] = 'http://another.image.png'
        helpers.call_action(u'package_update', **dataset)

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll changed "{}" {}'
             .format(dataset['name'], dataset['id'])])

    def test_deleted_resource_leads_to_queued_task(self):
        dataset = factories.Dataset(resources=[
            {'url': 'http://some.image.png', 'format': 'png'}])
        helpers.call_action(u'job_clear')

        dataset['resources'] = []
        helpers.call_action(u'package_update', **dataset)

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll changed "{}" {}'
             .format(dataset['name'], dataset['id'])])

    def test_created_dataset_leads_to_queued_task(self):
        dataset = {'name': 'testdataset_da',
                   'title': 'Test Dataset',
                   'notes': 'Just another test dataset.',
                   'resources': [
                        {'url': 'http://some.image.png', 'format': 'png'}
                   ]}
        dataset = helpers.call_action(u'package_create', **dataset)
        # this should prompt datapackage.json to be updated

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll new "{}" {}'
             .format(dataset['name'], dataset['id'])])

    def test_changed_dataset_leads_to_queued_task(self):
        dataset = factories.Dataset(resources=[
            {'url': 'http://some.image.png', 'format': 'png'}])
        helpers.call_action(u'job_clear')

        dataset['notes'] = 'Changed description'
        helpers.call_action(u'package_update', **dataset)
        # this should prompt datapackage.json to be updated

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll changed "{}" {}'
             .format(dataset['name'], dataset['id'])])

    def test_creation_of_zip_resource_leads_to_queued_task(self):
        # but we don't get an infinite loop because it is stopped by the
        # skip_if_no_changes
        dataset = factories.Dataset(resources=[
            {'url': 'http://some.image.png', 'format': 'png'}])
        helpers.call_action(u'job_clear')
        resource = {
            'package_id': dataset['id'],
            'name': 'All resource data',
            # no need to have an upload param in this test
            'downloadall_metadata_modified': dataset['metadata_modified'],
        }
        helpers.call_action(u'resource_create', **resource)

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll changed "{}" {}'
             .format(dataset['name'], dataset['id'])])

    def test_other_instance_types_do_nothing(self):
        factories.User()
        factories.Organization()
        factories.Group()
        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [])

        assert_equal(list(helpers.call_action(u'job_list')), [])

    # An end-to-end test is too tricky to write - creating a dataset and seeing
    # the zip file created requires the queue worker to run, but that rips down
    # the existing database session. And if we use the synchronous_enqueue_job
    # mock, then when it creates the resoure for the zip it closes the db
    # session, which is not allowed during a
    # DomainObjectModificationExtension.notify(). So we just do unit tests for
    # adding the zip task to the queue, and testing the task (test_tasks.py)
