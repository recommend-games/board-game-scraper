from datetime import date, datetime, time
from typing import Any

from itemadapter import ItemAdapter
from scrapy.exporters import JsonLinesItemExporter
from scrapy.utils.serialize import ScrapyJSONEncoder


class ISODateTimeJSONEncoder(ScrapyJSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, o: Any) -> Any:
        """
        Serialize the given object to JSON, using ISO date and time formats for
        date, datetime and time objects.
        """

        if isinstance(o, (date, datetime, time)):
            return o.isoformat()

        return super().default(o)


class SparseJsonLinesItemExporter(JsonLinesItemExporter):
    """Recursively remove falsey values from items before exporting them."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.encoder = ISODateTimeJSONEncoder(**self._kwargs)

    def remove_falsey_values(self, item: Any) -> None:
        """Recursively remove falsey values from the given item."""

        adapter = ItemAdapter(item)

        for key, value in tuple(adapter.items()):
            if not value:
                del adapter[key]

            elif ItemAdapter.is_item(value):
                self.remove_falsey_values(value)

            elif isinstance(value, (list, tuple, set, frozenset)):
                for v in value:
                    if ItemAdapter.is_item(v):
                        self.remove_falsey_values(v)

    def export_item(self, item: Any) -> None:
        """Recursively remove falsey values from the given item before exporting it."""

        self.remove_falsey_values(item)

        return super().export_item(item)
