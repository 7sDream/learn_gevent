import os
import sqlite3


def create_or_get(name):
    need_create_table = True if not os.path.isfile(name) else False
    conn = sqlite3.connect(name)
    if need_create_table:
        conn.execute("""CREATE TABLE user (
              id              TEXT  PRIMARY KEY  NOT NULL,
              name            TEXT                NOT NULL,
              headline        TEXT                NOT NULL,
              gender          INT                 NOT NULL,
              answer_count    INT                 NOT NULL,
              question_count  INT                 NOT NULL,
              voteup_count    INT                 NOT NULL,
              thanked_count   INT                 NOT NULL,
              following_count INT                 NOT NULL,
              follower_count  INT                 NOT NULL,
              school          TEXT                NOT NULL,
              major           TEXT                NOT NULL,
              address         TEXT                NOT NULL,
              industry        TEXT                NOT NULL,
              company         TEXT                NOT NULL,
              job             TEXT                NOT NULL
            );""")
        conn.commit()
    return conn
