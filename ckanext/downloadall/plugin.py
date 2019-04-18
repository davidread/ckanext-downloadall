import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from tasks import update_zip


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

        print('{} {} {}'.format(operation, type(entity), entity.name))

        # add this dataset to the queue, if not already on there
        toolkit.enqueue_job(
            update_zip, [entity.id],
            title=u'DownloadAll {} "{}"'.format(operation, entity.name))
