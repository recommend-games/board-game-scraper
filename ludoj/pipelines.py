# -*- coding: utf-8 -*-

''' Scrapy item pipelines '''

from scrapy.exceptions import DropItem


class ValidatePipeline:
    ''' validate items '''

    # pylint: disable=no-self-use,unused-argument
    def process_item(self, item, spider):
        ''' verify if all required fields are present '''

        if all(item.get(field) for field in item.fields if item.fields[field].get('required')):
            return item

        raise DropItem('Missing required field in {}'.format(item))


class DataTypePipeline:
    ''' convert fields to their required data type '''

    # pylint: disable=no-self-use,unused-argument
    def process_item(self, item, spider):
        ''' convert to data type '''

        for field in item.fields:
            dtype = item.fields[field].get('dtype')
            default = item.fields[field].get('default', NotImplemented)

            if item.get(field) is None and default is not NotImplemented:
                item[field] = default() if callable(default) else default

            if not dtype or item.get(field) is None or isinstance(item[field], dtype):
                continue

            try:
                item[field] = dtype(item[field])
            except Exception as exc:
                if default is NotImplemented:
                    raise DropItem(
                        'Could not convert field "{}" to datatype "{}" in item "{}"'
                        .format(field, dtype, item)) from exc

                item[field] = default() if callable(default) else default

        return item
