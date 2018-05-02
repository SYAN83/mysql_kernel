import pymysql
import pandas as pd
import re
from .grader import MySQLAutoTest
import pprint
from collections import namedtuple


Message = namedtuple('Message', ['msg_type', 'content'])


def error_content(e):
    content = {'ename': e.__class__.__name__,
               'evalue': e.__str__()}
    traceback = ['\x1b[0;31m{ename}\x1b[0m: {evalue}'.format(**content)]
    content['traceback'] = traceback
    return content


class MySQLReader(object):
    """A low level MySQL API which send query to MySQL database and fetch data.
    """
    _conn = None
    _data = None
    _soln = None

    def __init__(self, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 3306)
        self.user = kwargs.get('user', 'root')
        self.password = kwargs.get('password', '')
        self._connect()

    def run(self, code):
        if re.search('AutoTest', code, re.IGNORECASE):
            for msg in self.run_grader(test_code=code):
                yield msg
        else:
            for msg in self.run_query(sql_code=code):
                yield msg

    def run_query(self, sql_code, soln=False):
        for query in filter(len, (query.strip() for query in sql_code.strip().split(';'))):
            try:
                result = self._execute(query=query)
            except pymysql.err.MySQLError as e:
                yield Message(msg_type='error',
                              content=error_content(e))
            else:
                msg = self._format(result=result, soln=soln)
                if isinstance(msg, dict):
                    yield Message(msg_type='display_data',
                                  content={'data': msg})
                else:
                    yield Message(msg_type='stream',
                                  content={'name': 'stdout', 'text': msg})

    def run_grader(self, test_code):
        lines = test_code.strip().split('\n')
        for i in range(len(lines)):
            if re.search('AutoTest', lines[i], re.IGNORECASE):
                sql_soln = '\n'.join(lines[:i])
                testcases = filter(len, [line.strip() for line in lines[i+1:]])
                break
        else:
            return
        # run solution query
        for msg in self.run_query(sql_code=sql_soln, soln=True):
            yield msg
        # run autotest
        test = MySQLAutoTest(df1=self._data, df2=self._soln)
        count = 0
        for line in testcases:
            if line and re.search(r'test.assert\w+\(.*\)', line):
                try:
                    msg = eval(line)
                except Exception as e:
                    yield Message(msg_type='error',
                                  content=error_content(e))
                    break
                else:
                    if msg is None:
                        msg = 'OK'
                    count += 1
                    yield Message(msg_type='stream',
                                  content={'name': 'stdout', 'text': msg})
        else:
            yield Message(msg_type='execute_result',
                          content={'data':{'text/plain':'AutoTest cases passed: {}'.format(count)}})

    def close(self):
        """Close database connection
        :return: None
        """
        self._conn.close()

    def _connect(self):
        """Connect to MySQL server.
        :return: None
        """
        self._conn = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     charset="utf8mb4",
                                     cursorclass=pymysql.cursors.DictCursor,
                                     autocommit=True)

    def _execute(self, query):
        """Send query to MySQL database and fetch output, reconnect to database if disconnected.
        :param query: A MySQL supported query.
        :return: Fetched data.
        """
        self._conn.ping(reconnect=True)
        with self._conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result

    def _format(self, result, soln=False):
        if isinstance(result, list):
            table = pd.DataFrame(result)
            if soln:
                self._soln = table
            else:
                self._data = table
            return {'text/html': table.to_html(),
                    'text/plain': table.to_string()}
        elif not result:
            return ''
        else:
            return str(result)


if __name__ == '__main__':
    import yaml, time

    with open('/Users/shuyan/.local/config/mysql_config.yml', 'r') as f:
        CONFIG = yaml.load(f)

    reader = MySQLReader(**CONFIG)
    time.sleep(5)
    print(reader._execute("Show databases;"))

    # print(reader._conn.ping(reconnect=True))

    # queries = """
    # USE movies_db;
    # SELECT movie_title, language, genres, imdb_score FROM movies LIMIT 2;
    # """
    # for msg in reader.run(code=queries):
    #     pprint.pprint(msg)


    # print('*********************')
    #
    # test_code = """
    # AutoTest:
    #
    # test.assertTableFetched()
    # test.assertRowNumEqual(num=2)
    # test.assertColumnNumEqual(num=4)
    # test.assertColumnIncludeOnly(cols=[\'movie_title\', \'language\', \'genres\', \'imdb_score\'])
    # """
    # for msg in reader.run(code=test_code):
    #     pprint.pprint(msg)
