# encoding: utf-8

import click

from ckan.cli import (
    click_config_option, load_config
)
from ckan.config.middleware import make_app
from ckan.plugins.toolkit import get_action
from ckan import model

import tasks


class MockTranslator(object):
    def gettext(self, value):
        return value

    def ugettext(self, value):
        return value

    def ungettext(self, singular, plural, n):
        if n > 1:
            return plural
        return singular


class CkanCommand(object):

    def __init__(self, conf=None):
        self.config = load_config(conf)

        # package_update needs a translator defined i.e. _()
        from paste.registry import Registry
        import pylons
        registry = Registry()
        registry.prepare()
        registry.register(pylons.translator, MockTranslator())

        self.app = make_app(self.config.global_conf, **self.config.local_conf)


@click.group()
@click.help_option(u'-h', u'--help')
@click_config_option
@click.pass_context
def cli(ctx, config, *args, **kwargs):
    ctx.obj = CkanCommand(config)


@cli.command(u'update-zip', short_help=u'Update zip file for a dataset')
@click.argument('dataset_ref')
def update_zip(dataset_ref):
    u''' update-zip <package-name>

    Generates zip file for a dataset, downloading its resources.'''
    tasks.update_zip(dataset_ref)
    click.secho(u'update-zip: SUCCESS', fg=u'green', bold=True)


@cli.command(u'update-all-zips',
             short_help=u'Update zip files for all datasets')
def update_all_zips():
    u''' update-all-zips <package-name>

    Generates zip file for all datasets. It is done synchronously.'''
    context = {'model': model, 'session': model.Session}
    datasets = get_action('package_list')(context, {})
    for i, dataset_name in enumerate(datasets):
        print('Processing dataset {}/{}'.format(i + 1, len(datasets)))
        tasks.update_zip(dataset_name)
    click.secho(u'update-all-zips: SUCCESS', fg=u'green', bold=True)
