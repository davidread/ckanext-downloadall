"""Tests for plugin.py."""
import mock
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

    def test_new_dataset_leads_to_queued_task(self):
        dataset = factories.Dataset()
        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll new "{}"'.format(dataset['name'])])

    def test_changed_dataset_leads_to_queued_task(self):
        dataset = factories.Dataset()
        helpers.call_action(u'job_clear')

        dataset['title'] = 'New title'
        helpers.call_action(u'package_update', **dataset)

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
            [u'DownloadAll changed "{}"'.format(dataset['name'])])

    # An end-to-end test is too tricky to write - creating a dataset and seeing
    # the zip file created requires the queue worker to run, but that rips down
    # the existing database session. And if we use the synchronous_enqueue_job
    # mock, then when it creates the resoure for the zip it closes the db
    # session, which is not allowed during a
    # DomainObjectModificationExtension.notify(). So we just do unit tests for
    # adding the zip task to the queue, and testing the task (test_tasks.py)
