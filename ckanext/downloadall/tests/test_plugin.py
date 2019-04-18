"""Tests for plugin.py."""
import mock
from nose.tools import assert_equal

import ckanext.downloadall.plugin as plugin
from ckan.tests import factories
from ckan.tests import helpers


def synchronous_enqueue_job(job_func, args=None, kwargs=None, title=None):
    '''
    Synchronous mock for ``ckan.plugins.toolkit.enqueue_job``.
    '''
    args = args or []
    kwargs = kwargs or {}
    job_func(*args, **kwargs)


class TestNotify(object):
    @classmethod
    def setupClass(cls):
        helpers.reset_db()

    def setup(self):
        helpers.call_action(u'job_clear')

    def test_new_dataset_leads_to_queued_task(self):
        dataset = factories.Dataset()
        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
             [u'DownloadAll new "{}"'.format(dataset['name']),
              u'DownloadAll changed "{}"'.format(dataset['name'])])

    def test_changed_dataset_leads_to_queued_task(self):
        dataset = factories.Dataset()
        helpers.call_action(u'job_clear')

        dataset['title'] = 'New title'
        helpers.call_action(u'package_update', **dataset)

        assert_equal(
            [job['title'] for job in helpers.call_action(u'job_list')],
             [u'DownloadAll changed "{}"'.format(dataset['name'])])

    @mock.patch('ckan.plugins.toolkit.enqueue_job',
                side_effect=synchronous_enqueue_job)
    def test_new_dataset_has_zip_added(self, enqueue_job_mock):
        factories.Dataset()
        # NB exceptions get swallowed by notify
