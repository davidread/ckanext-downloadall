import re

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from ckan import model

from tasks import update_zip


log = __import__('logging').getLogger()


class DownloadallPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification)

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

        if isinstance(entity, model.Package):
            dataset_name = entity.name
        elif isinstance(entity, model.Resource):
            dataset_name = entity.related_packages()[0].name
        else:
            return
        print('{} {} {}'.format(operation, type(entity), entity.name))

        # skip task if the dataset is already queued
        queue = DEFAULT_QUEUE_NAME
        jobs = toolkit.get_action('job_list')(
            {'ignore_auth': True}, {'queues': [queue]})
        if jobs:
            for job in jobs:
                match = re.match(r'DownloadAll \w+ "([^"]*)"', job[u'title'])
                if match:
                    queued_dataset_name = match.groups()[0]
                    if dataset_name == queued_dataset_name:
                        log.info('Already queued dataset "{}"'
                                 .format(dataset_name))
                        return

        # add this dataset to the queue
        toolkit.enqueue_job(
            update_zip, [entity.id],
            title=u'DownloadAll {} "{}"'.format(operation, entity.name),
            queue=queue)
