import sqlalchemy
import mcauto.core.database
from datetime import date

class QAFailedError(ValueError):
    pass
class DateQAFailedError(QAFailedError):
    pass
class SumQAFailedError(QAFailedError):
    pass


class BaseTask():
    """
    Defines class methods for subclasses to override.
    """
    def __init__(self):
        pass

    def __str__(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError


class BaseTaskGroup():
    """
    Takes a list of classes and runs them
    """
    def __init__(self, tasks):
        self.tasks = tasks
        pass

    def run(self):
        for task in self.tasks:
            task.run()

class BaseDatabaseQATask(BaseTask):

    def __init__(self, engine: sqlalchemy.engine.Engine, proc: str, params: list):
        super().__init__()
        self.engine = engine
        self.proc = proc
        self.params = params

    def run(self):
        self.fetch_data()
        self.check()

    def check(self):
        raise NotImplementedError

    def fetch_data(self, in_thread=False):
        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()
            arg_fmt = " " + ", ".join(['?' for i in self.params])
            cursor.execute(self.proc + arg_fmt, self.params)
            self.results = list(cursor.fetchall())
            cursor.close()
        finally:
            conn.close()


class DatabaseSumQATask(BaseDatabaseQATask):
    """
    After calling procedure, makes sure that results[0][0] == results[0][1].
    This primarily tests DCM Conversions pivots.
    """
    def __init__(self, cast_as_type=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not isinstance(cast_as_type, type):
            cast_as_type = lambda x: x
        self.cast_as_type=cast_as_type

    def check(self):
        if not hasattr(self, 'results'):
            self.fetch_data()
        try:
            assert self.cast_as_type(self.results[0][0]) == self.cast_as_type(self.results[0][1])
        except AssertionError as e:
            raise SumQAFailedError(f"{self.results[0][0]} != {self.results[0][1]}") from e
        except ValueError as e:
            raise SumQAFailedError(f"{self.results[0][0]} or {self.results[0][1]} failed conversion to {self.cast_as_type}") from e


class DatabaseDateQATask(BaseDatabaseQATask):
    date_fmt = '%Y-%m-%d'
    def __init__(self, start_date: date, end_date: date, name: str, *args, **kwargs):
        self.name = name
        params = [name]
        #params = [start_date.strftime(self.date_fmt), end_date.strftime(self.date_fmt)]
        super().__init__(*args, **kwargs, params=params)
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        self.fetch_data()
        self.check()

    def check(self):
        if not hasattr(self, 'results'):
            self.fetch_data()
        try:
            assert len(self.results) == 1
            assert self.results[0][0] == self.start_date
            assert self.results[0][1] == self.end_date
        except AssertionError as e:
            raise DateQAFailedError(f'Date QA failed on source {self.name}') from e

class BaseProcedure():
    def __init__(self):
        pass

    def run(self):
        pass

class DateLimitedProcedure(BaseProcedure):
    def __init__(self, get_latest_date_in_tbl_proc, date_parameter_proc):
        self.get_latest_date_in_tbl_proc = get_latest_date_in_tbl_proc
        self.date_parameter_proc = date_parameter_proc

class ToBeNamedObject():
    def __init__(self, before_qa: BaseTaskGroup, action_proc: BaseProcedure, after_qa: BaseTaskGroup, undo_proc: BaseProcedure):
        pass