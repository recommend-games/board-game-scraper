# -*- coding: utf-8 -*-

''' Scrapy item pipelines '''

from __future__ import unicode_literals

from scrapy.exceptions import DropItem


class ValidatePipeline(object):
    ''' validate items '''

    # pylint: disable=no-self-use,unused-argument
    def process_item(self, item, spider):
        ''' verify if all required fields are present '''

        if all(item.get(field) for field in item.fields if item.fields[field].get('required')):
            return item

        raise DropItem('Missing required field in {:s}'.format(item))

class DataTypePipeline(object):
    ''' convert fields to their required data type '''

    # pylint: disable=no-self-use,unused-argument
    def process_item(self, item, spider):
        ''' convert to data type '''

        for field in item.fields:
            dtype = item.fields[field].get('dtype')

            if dtype and item.get(field) is not None:
                try:
                    item[field] = dtype(item[field])
                except Exception as exc:
                    if 'default' in item.fields[field]:
                        item[field] = item.fields[field]['default']
                    else:
                        raise DropItem('Could not convert field "{:s}" to datatype "{:s}" '
                                       'in item "{}"'.format(field, dtype, item)) from exc

        return item
