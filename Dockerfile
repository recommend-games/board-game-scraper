FROM python:3.8.6

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV MAILTO=''
ENV PYTHONPATH=.

RUN mkdir --parents /app
WORKDIR /app

RUN python3.8 -m pip install --upgrade \
        pipenv==2020.11.4
COPY Pipfile* ./
RUN pipenv install --deploy --verbose

COPY . .

ENTRYPOINT ["pipenv", "run"]
CMD ["scrapy", "list"]
