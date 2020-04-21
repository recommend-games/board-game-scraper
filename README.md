# board-game-scraper

Scraping data about board games from the web. View the data live at
[Recommend.Games](https://recommend.games/)! Install via

```bash
pip install board-game-scraper
```

## Sources

* [Board Game Atlas](https://www.boardgameatlas.com/) (`bga`)
* [BoardGameGeek](https://boardgamegeek.com/) (`bgg`)
* [DBpedia](https://wiki.dbpedia.org/) (`dbpedia`)
* [Luding.org](https://luding.org/) (`luding`)
* [Spielen.de](https://gesellschaftsspiele.spielen.de/) (`spielen`)
* [Wikidata](https://www.wikidata.org/) (`wikidata`)

## Run scrapers

[Requires Python 3](https://pythonclock.org/). Make sure
[Pipenv](https://docs.pipenv.org/) is installed and create the virtual
environment:

```bash
python3 -m pip install --upgrade pipenv
pipenv install --dev
pipenv shell
```

Run a spider like so:

```bash
JOBDIR="jobs/${SPIDER}/$(date --utc +'%Y-%m-%dT%H-%M-%S')"
scrapy crawl "${SPIDER}" \
    --output 'feeds/%(name)s/%(time)s/%(class)s.csv' \
    --set "JOBDIR=${JOBDIR}"
```

where `$SPIDER` is one of the IDs above.

Run all the spiders with the [`run_scrapers.sh`](run_scrapers.sh) script. Get a
list of the running scrapers' PIDs with the [`processes.sh`](processes.sh)
script. You can close all the running scrapers via

```bash
./processes.sh stop
```

and resume them later.

## Tests

You can run `scrapy check` to perform contract tests for all spiders, or
`scrapy check $SPIDER` to test one particular spider. If tests fails,
there most likely has been some change on the website and the spider needs
updating.

## Board game datasets

If you are interested in using any of the datasets produced by this scraper,
take a look at the
[BoardGameGeek guild](https://boardgamegeek.com/thread/2287371/boardgamegeek-games-and-ratings-datasets).
A subset of the data can also be found on [Kaggle](https://www.kaggle.com/mshepherd/board-games).

## Links

* [board-game-scraper](https://gitlab.com/recommend.games/board-game-scraper):
 This repository
* [Recommend.Games](https://recommend.games/): board game recommender using the
 scraped data
* [recommend-games-server](https://gitlab.com/recommend.games/recommend-games-server):
 Server code for [Recommend.Games](https://recommend.games/)
* [board-game-recommender](https://gitlab.com/recommend.games/board-game-recommender):
 Recommender code for [Recommend.Games](https://recommend.games/)
