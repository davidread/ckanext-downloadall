import re

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from ckan import model

from tasks import update_zip
import helpers


log = __import__('logging').getLogger(__name__)


class DownloadallPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification)
    plugins.implements(plugins.ITemplateHelpers)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'downloadall')

    # IDomainObjectModification

    def notify(self, entity, operation):
        u'''
        Send a notification on entity modification.

        :param entity: instance of module.Package.
        :param operation: 'new', 'changed' or 'deleted'.
        '''
        if operation == 'deleted':
            return

        log.debug(u'{} {} \'{}\''
                  .format(operation, type(entity).__name__, entity.name))
        if isinstance(entity, model.Package):
            # don't need to know about changes to the package, just its
            # resources. Indeed change to the zip resource also trigger a
            # notify() on the package, and reacting would cause an infinite
            # loop
            return
        elif isinstance(entity, model.Resource):
            if entity.extras.get('downloadall_metadata_modified'):
                # this is the zip of all the resources - no need to react to
                # it being changed
                log.debug('Ignoring change to zip resource')
                return
            dataset_name = entity.related_packages()[0].name
            dataset_id = entity.related_packages()[0].id
        else:
            return

        # skip task if the dataset is already queued
        queue = DEFAULT_QUEUE_NAME
        jobs = toolkit.get_action('job_list')(
            {'ignore_auth': True}, {'queues': [queue]})
        if jobs:
            for job in jobs:
                match = re.match(
                    r'DownloadAll \w+ "[^"]*" ([\w-]+)', job[u'title'])
                if match:
                    queued_dataset_id = match.groups()[0]
                    if dataset_id == queued_dataset_id:
                        log.info('Already queued dataset: {} {}'
                                 .format(dataset_name, dataset_id))
                        return

        # add this dataset to the queue
        log.debug(u'Queuing job update_zip: {} {}'
                  .format(operation, dataset_name))

        toolkit.enqueue_job(
            update_zip, [dataset_id],
            title=u'DownloadAll {} "{}" {}'.format(operation, dataset_name,
                                                   dataset_id),
            queue=queue)

    # ITemplateHelpers

    def get_helpers(self):
        return {
            'downloadall__pop_zip_resource': helpers.pop_zip_resource,
        }
