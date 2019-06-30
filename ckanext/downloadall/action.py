import ckan.plugins as p
from ckan import model

import plugin


@p.toolkit.chained_action  # requires CKAN 2.7+
def datastore_create(original_action, context, data_dict):
    # This gets called when xloader or datapusher loads a new resource or
    # data dictionary is changed. We need to regenerate the zip when the latter
    # happens, and it's ok if it happens at the other times too.
    result = original_action(context, data_dict)

    # update the zip
    if 'resource_id' in data_dict:
        res = model.Resource.get(data_dict['resource_id'])
        if res:
            dataset = res.related_packages()[0]
            plugin.enqueue_update_zip(dataset.name, dataset.id,
                                      'datastore_create')

    return result
