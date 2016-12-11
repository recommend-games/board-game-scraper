# -*- coding: utf-8 -*-

from scrapy import Field, Item

class GameItem(Item):
    name = Field(required=True)
    year = Field(dtype=int, default=None)
    designer = Field()
    publisher = Field()
    game_type = Field()
    url = Field()
    image_url = Field()
    link = Field()
    rank = Field(dtype=int, default=None)
    geek_rating = Field(dtype=float, default=None)
    avg_rating = Field(dtype=float, default=None)
    num_votes = Field(dtype=int, default=0)
