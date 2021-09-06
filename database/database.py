import logging
from typing import Generator, Optional

import psycopg2
from psycopg2.extensions import connection, cursor
from datetime import datetime

from psycopg2.extras import DictCursor

from model.RawArticle import RawArticle

database_config = {
    "dbname": "nywspaper",
    "user": "nywspaper",
    "password": "nywspaperpassword",  # TODO Read this from config
    "host": "localhost",
    "port": 5433
}


def connect(dbconfig: dict = None) -> Optional[connection]:
    if dbconfig is None:
        dbconfig = database_config

    try:
        return psycopg2.connect(**dbconfig)
    except Exception as e:
        logging.error("Could not connect to database", e)
        return None


def get_raw_articles(conn: connection, after: datetime = None) -> Generator[RawArticle, None, None]:
    cur: cursor = conn.cursor(cursor_factory=DictCursor)
    if after is None:
        after = datetime(year=1, month=1, day=1)

    cur.execute("""SELECT 
                        id, 
                        date_published, 
                        source_domain, 
                        title, 
                        maintext, 
                        description, 
                        image_url, 
                        authors, 
                        language 
                    FROM currentversions 
                    WHERE date_publish > %s
                        AND language = 'en'""",
                (after,))

    for res in cur:
        yield RawArticle(**res)

def delete_duplicates(conn: connection) -> None:
    """
    This is a method to remove duplicate articles. It probably doesn't find all the duplicates.
    It removes the articles who have > 90% levenshtein similarity with their neighbor when ordered
    by article text.
    :param conn: psycopg2 connection to the database
    :return:     None
    """

    sql = """with dupe_score as (
        select  
            1 - levenshtein(left(maintext, 255), left(lag(maintext) over (order by left(maintext, 255)), 255))::float / 255 as duplication_score, 
            *,
            lag(maintext) over (order by left(maintext, 255)) as other from currentversions c
        order by left(maintext, 255)
        limit 1000000
        )
        , dupes as (select id from dupe_score where duplication_score > 0.9)
        delete from currentversions c2 where id in (select * from dupes)"""
    cur: cursor = conn.cursor()
    cur.execute(sql)

