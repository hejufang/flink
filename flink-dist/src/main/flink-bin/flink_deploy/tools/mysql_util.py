import MySQLdb
import random

from pyutil.program.conf import Conf
from util import red, green


class MysqlUtil(object):
    def __init__(self):
        self.db = self.init_db()

    def init_db(self):
        conf = Conf("/opt/tiger/ss_conf/ss/db_dayu.conf")
        hosts = conf.get_values('ss_dayu_write_host')
        r = random.randint(0, len(hosts) - 1)
        host = hosts[r]
        port = conf.get('ss_dayu_write_port')
        user = conf.get('ss_dayu_write_user')
        database = conf.get('ss_dayu_db_name')
        password = conf.get('ss_dayu_write_password')
        db = MySQLdb.connect(host=host, port=port, user=user, passwd=password,
                             db=database)
        return db

    def update_job_meta(self, job_meta):
        columns = job_meta.keys()
        column_names = ",".join(columns)
        values = []
        for column in columns:
            if job_meta[column]:
                value = job_meta[column]
            else:
                print "Value of %s is None" + column
                value = ""
            values.append("'" + value + "'")
        value_str = ",".join(values)
        sql = "insert into flink_job_meta(" + column_names + ") values (" + \
              value_str + ")"
        print "sql = %s" % sql
        result = self.execute_sql(sql)
        if result:
            print green("Success to save job meta to database.")
        else:
            print red(
                "Failed to save job meta to database. jobmeta = %s" % job_meta)

    def execute_sql(self, sql):
        success = False
        cur = self.db.cursor()
        try:
            self.db.ping()
        except Exception, e:
            self.db = self.init_db()
            cur = self.db.cursor()
        try:
            res = cur.execute(sql)
            self.db.commit()
            success = True
        except Exception, e:
            print "Failed to execute sql: %s" % sql
            print e
            success = False
        finally:
            cur.close()
        return success

    def query(self, sql):
        cur = self.db.cursor(MySQLdb.cursors.DictCursor)
        try:
            self.db.ping()
        except Exception, e:
            self.db = self.init_db()
            cur = self.db.cursor()
        try:
            cur.execute(sql)
            result_set = cur.fetchall()
            self.db.commit()
            return list(result_set)
        except Exception, e:
            print "Failed to execute sql: %s" % sql
            print e
        finally:
            cur.close()
