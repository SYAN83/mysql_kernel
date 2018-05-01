import sys
import functools
import pandas as pd


def catch_error(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            data = func(*args, **kwargs)
        except AutoTestCaseError as e:
            print(e)
        else:
            return data
    return wrapper


class MySQLAutoTest(object):

    def __init__(self, df1=None, df2=None):
        self.df1 = df1
        self.df2 = df2

    def update_solution(self, df2):
        self.df2 = df2

    def assertTableFetched(self, **kwargs):
        """
        Check table is fetched
        :param kwargs:
        :return:
        """
        if self.df1 is None:
            raise AutoTestCaseError(**kwargs)

    def assertSolutionExists(self, **kwargs):
        """
        Check table is available
        :param kwargs:
        :return:
        """
        if self.df2 is None:
            raise AutoTestCaseError(**kwargs)

    def assertColumnNumEqual(self, num=None, **kwargs):
        """
        Check the number of columns
        :param num:
        :param kwargs:
        :return:
        """
        self.assertTableFetched()
        if num is None:
            num = self.df2.shape[1]
        if self.df1.shape[1] != num:
            raise AutoTestCaseError(a=self.df1.shape[1], b=num, **kwargs)

    def assertColumnInclude(self, cols=[], **kwargs):
        """
        Check table contains columns
        :param cols:
        :return:
        """
        self.assertTableFetched()
        if cols == []:
            cols = self.df2.columns
        diff = set(cols) - set(self.df1.columns)
        if diff:
            raise AutoTestCaseError(diff=diff, **kwargs)

    def assertColumnExclude(self, cols=[], **kwargs):
        """
        Check table not missing columns
        :param cols:
        :return:
        """
        self.assertTableFetched()
        intersection = set(cols).intersection(self.df1.columns)
        if intersection:
            raise AutoTestCaseError(diff=intersection, **kwargs)

    def assertColumnIncludeOnly(self, cols=[], **kwargs):
        """
        Check columns are strickly equal
        :param cols:
        :param kwargs:
        :return:
        """
        self.assertTableFetched()
        if cols == []:
            cols = self.df2.columns
        col_1 = set(self.df1.columns)
        col_2 = set(cols)

        a = col_1 - col_2
        b = col_2 - col_1
        if a or b:
            raise AutoTestCaseError(a=a, b=b, **kwargs)


    def assertRowNumEqual(self, num=None, **kwargs):
        """
        Check the number of rows
        :param num:
        :param kwargs:
        :return:
        """
        self.assertTableFetched()
        if num is None:
            num = self.df2.shape[0]
        if self.df1.shape[0] != num:
            raise AutoTestCaseError(a=self.df1.shape[0], b=num, **kwargs)

    def assertRowEqual(self, strict=True, **kwargs):
        """
        Compare rows between df1 and df2.
        If strict, then row orders are also taken into account.
        :param num:
        :param kwargs:
        :return:
        """
        self.assertTableFetched()
        if strict:
            temp = ~self.df1.isin(self.df2).all(axis=1)
            diff = list(map(str, temp.index[temp].tolist()))
            if diff:
                raise AutoTestCaseError(diff=diff, **kwargs)
        else:
            df1 = set(tuple(sorted(row.items())) for row in self.df1.to_dict(orient='row'))
            df2 = set(tuple(sorted(row.items())) for row in self.df2.to_dict(orient='row'))
            a, b = df1 - df2, df2 - df1
            if a or b:
                raise AutoTestCaseError(a=a, b=b, **kwargs)



class AutoTestCaseError(Exception):

    def __new__(cls, *args, **kwargs):
        instance = Exception.__new__(cls, *args, **kwargs)
        cls.messager = MySQLAutoTestErrorMessages()
        return instance

    def __init__(self, **kwargs):
        self.msg = ''
        if kwargs.get('pre_msg'):
            self.msg = kwargs.get('pre_msg') + '\n'
        if kwargs.get('msg') is None:
            self.msg += self.messager.msg(func=sys._getframe(1).f_code.co_name, **kwargs)
        else:
            self.msg += kwargs.get('msg')
        if kwargs.get('post_msg'):
            self.msg += '\n' + kwargs.get('post_msg')

    def __str__(self):
        return self.msg


class MySQLAutoTestErrorMessages(object):

    assertTableFetched = 'Your query did not fetched any table!'
    assertSolutionExists = 'Table does not exist or not available!'

    def msg(self, func, **kwargs):
        msg_obj = getattr(self, func, 'AutoTest failed!')
        if callable(msg_obj):
            return msg_obj(**kwargs)
        else:
            return msg_obj

    def assertColumnNumEqual(self, a, b, **kwargs):
        return 'Your query returns {} columns. The correct output should contain {} columns.'.format(a, b)

    def assertColumnInclude(self, diff, **kwargs):
        return 'The following columns are not available in your answer: {}.'.format(', '.join(diff))

    def assertColumnExclude(self, diff, **kwargs):
        return 'The following columns should not appear in your answer: {}.'.format(', '.join(diff))

    def assertColumnIncludeOnly(self, a, b, **kwargs):
        error_msg = ''
        if a:
            error_msg += 'The following columns should not appear in your answer: {}. '.format(', '.join(a))
        if b:
            error_msg += 'The following columns are not available in your answer: {}. '.format(', '.join(b))
        return error_msg

    def assertRowNumEqual(self, a, b, **kwargs):
        return 'Your query returns {} rows. The correct output should contain {} rows.'.format(a, b)

    def assertRowEqual(self, a=None, diff=None, **kwargs):
        if diff or a:
            return 'One or more rows are not correct.'
        else:
            return 'One or more rows are missing.'



if __name__ == '__main__':
    df1 = pd.DataFrame({'col1': [3,2,1], 'col2': ['c','b','a']})
    df2 = pd.DataFrame({'col2': ['a','b','c'], 'col3': [1,2,3]})
    test = MySQLAutoTest(df1=df1, df2=df2)
    try:
        eval('test.assertColumnIncludeOnly(cols=[\'col1\', \'col2\'])')
    except Exception as e:
        print(e)
    else:
        print('ok')
