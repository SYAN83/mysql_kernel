import pymysql
import pandas as pd
import re
from grader import MySQLAutoTest


class MySQLReader(object):
    """A low level MySQL API which send query to MySQL database and fetch data.
    """
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
                yield 'error', {'ename': e.__class__.__name__, 'evalue': e.__str__()}
            else:
                yield 'result', self._format(result=result, soln=soln)

    def run_grader(self, test_code):
        lines = test_code.strip().split('\n')
        for i in range(len(lines)):
            if re.search('AutoTest', lines[i], re.IGNORECASE):
                sql_soln = '\n'.join(lines[:i])
                testcases = lines[i+1:]
                break
        else:
            return
        # run solution query
        for msg in self.run_query(sql_code=sql_soln, soln=True):
            yield msg
        # run autotest
        test = MySQLAutoTest(df1=self._data, df2=self._soln)
        for line in testcases:
            if re.search(r'test.assert\w+\(\w*\s*\)', line):
                try:
                    eval(line)
                except Exception as e:
                    yield 'error', {'ename': e.__class__.__name__, 'evalue': e.__str__()}

    def close(self):
        """Close database connection
        :return: None
        """
        self.conn.close()

    def _connect(self):
        """Connect to MySQL server.
        :return: None
        """
        self.conn = pymysql.connect(host=self.host,
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
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result

    def _format(self, result, soln=False):
        if result is None:
            return True
        elif isinstance(result, list):
            table = pd.DataFrame(result)
            if soln:
                self._soln = table
            else:
                self._data = table
            return table.to_html()
        else:
            return result


if __name__ == '__main__':
    import yaml

    with open('./config.yml', 'r') as f:
        CONFIG = yaml.load(f)

    MYSQL_CONFIG = CONFIG['mysql_readonly']
    reader = MySQLReader(**MYSQL_CONFIG)
    queries = """
    SHOW databases;
    USE movies_db;
    SHOW tables;
    SELECT actor_2_name, budget 
    FROM movies LIMIT 3;
    """
    for msg in reader.run(code=queries):
        print(msg)

    print('*********************')

    test_code = """
    SELECT actor_1_name, budget
    FROM movies 
    LIMIT 4;
    
    AutoTest:
    
    test.assertTableFetched()
    test.assertRowNumEqual()
    test.assertColumnInclude()
    test.assertColumnIncludeOnly()
    """
    for msg in reader.run(code=test_code):
        print(msg)
