# -*- coding: utf-8 -*-

from scrapy import Field, Item

class GameItem(Item):
    name = Field(required=True)
    url = Field()
    image = Field()
    rank = Field(dtype=int, default=None)
    year = Field(dtype=int, default=None)
    geek_rating = Field(dtype=float, default=None)
    avg_rating = Field(dtype=float, default=None)
    num_votes = Field(dtype=int, default=0)
