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

    def test_creation_of_zip_resource_doesnt_lead_to_queued_task(self):
        # otherwise we'd get an infinite loop
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
            [])

    def test_other_instance_types_do_nothing(self):
        factories.Dataset()  # without resources
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
