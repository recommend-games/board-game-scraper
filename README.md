# ludoj #

Scraping data about board games from the web.

## Scraped websites ##

* [BoardGameGeek](https://boardgamegeek.com/) (`bgg`)
* [luding.org](http://luding.org/) (`luding`)
* [spielen.de](http://gesellschaftsspiele.spielen.de/) (`spielen`)

## Run scrapers ##

Requires Python 3. Make sure your (virtual) environment is up-to-date:

```bash
pip install -Ur requirements.txt
```

Run a spider like so:

```bash
scrapy crawl <spider> -o 'feeds/%(name)s/%(time)s/%(class)s.csv'
```

where `<spider>` is one of the IDs above.

You can run `scrapy check` to perform contract tests for all spiders, or
`scrapy check <spider>` to test one particular spider. If tests fails,
there most likely has been some change on the website and the spider needs
updating.
