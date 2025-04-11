import __init__
import unittest
import os

import utils.config_loader as config_loader
import db.postgresql as pg

class TestPgSQL(unittest.TestCase):
    def test_pgsql(self):
        config_loader.load_config()
        db = pg.PostgreSqlDB(config_loader.conf["admdb"])

        sql = "drop table if exists test_table1;"
        db.exec_sql(sql)

        sql = "create table if not exists test_table1 (id int, name varchar(10));"
        db.exec_sql(sql)

        db.truncate_table("test_table1")

        sql = "insert into test_table1 values(1, 'test1');"
        db.exec_sql(sql)

        cnt = db.count_table("test_table1")
        self.assertEqual(cnt, 1)

        sql = "drop table if exists test_table1;"
        db.exec_sql(sql)



if __name__ == "__main__":
    unittest.main()