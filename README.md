# 🎲 Board Game Scraper 🕸

[![PyPI](https://img.shields.io/pypi/v/board-game-scraper?style=flat-square)](https://pypi.python.org/pypi/board-game-scraper/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/board-game-scraper?style=flat-square)](https://pypi.python.org/pypi/board-game-scraper/)
[![PyPI - License](https://img.shields.io/pypi/l/board-game-scraper?style=flat-square)](https://pypi.python.org/pypi/board-game-scraper/)
[![Coookiecutter - Wolt](https://img.shields.io/badge/cookiecutter-Wolt-00c2e8?style=flat-square&logo=cookiecutter&logoColor=D4AA00&link=https://github.com/woltapp/wolt-python-package-cookiecutter)](https://github.com/woltapp/wolt-python-package-cookiecutter)

---

**Documentation**: [https://recommend-games.github.io/board-game-scraper](https://recommend-games.github.io/board-game-scraper)

**Source Code**: [https://github.com/recommend-games/board-game-scraper](https://github.com/recommend-games/board-game-scraper)

**PyPI**: [https://pypi.org/project/board-game-scraper/](https://pypi.org/project/board-game-scraper/)

---

Scraping data about board games from the web. View the data live at [Recommend.Games](https://recommend.games/)!

## Sources

* [BoardGameGeek](https://boardgamegeek.com/) (`bgg`)
* [DBpedia](https://wiki.dbpedia.org/) (`dbpedia`)
* [Luding.org](https://luding.org/) (`luding`)
* [Spielen.de](https://gesellschaftsspiele.spielen.de/) (`spielen`)
* [Wikidata](https://www.wikidata.org/) (`wikidata`)

## Board game datasets

If you are interested in using any of the datasets produced by this scraper, take a look at the
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

## Installation

```sh
pip install board-game-scraper
```

## Development

* Clone this repository
* Requirements:
  * [Poetry](https://python-poetry.org/)
  * Python 3.8+
* Create a virtual environment and install the dependencies

```sh
poetry install
```

* Activate the virtual environment

```sh
poetry shell
```

### Testing

```sh
pytest
```

### Documentation

The documentation is automatically generated from the content of the [docs directory](https://github.com/recommend-games/board-game-scraper/tree/master/docs) and from the docstrings
 of the public signatures of the source code. The documentation is updated and published as a [Github Pages page](https://pages.github.com/) automatically as part each release.

### Releasing

Trigger the [Draft release workflow](https://github.com/recommend-games/board-game-scraper/actions/workflows/draft_release.yml)
(press _Run workflow_). This will update the changelog & version and create a GitHub release which is in _Draft_ state.

Find the draft release from the
[GitHub releases](https://github.com/recommend-games/board-game-scraper/releases) and publish it. When
 a release is published, it'll trigger [release](https://github.com/recommend-games/board-game-scraper/blob/master/.github/workflows/release.yml) workflow which creates PyPI
 release and deploys updated documentation.

### Pre-commit

Pre-commit hooks run all the auto-formatting (`ruff format`), linters (e.g. `ruff` and `mypy`), and other quality
 checks to make sure the changeset is in good shape before a commit/push happens.

You can install the hooks with (runs for each commit):

```sh
pre-commit install
```

Or if you want them to run only for each push:

```sh
pre-commit install -t pre-push
```

Or if you want e.g. want to run all checks manually for all files:

```sh
pre-commit run --all-files
```

---

This project was generated using the [wolt-python-package-cookiecutter](https://github.com/woltapp/wolt-python-package-cookiecutter) template.
