import datetime, functools
from mcauto.core.database.database import SQLAlchemyUtils
from mcauto.config.config import DEFAULT_SAVE_FILEPATH
from mcauto.core.database.database import DBClassMixin
import pandas as pd
import pickle
import pyodbc

class DownloadFailedError(RuntimeError):
    pass


def call_setup(func):
    """
    Decorator that calls setup() on API instantiation if connection is aborted.
    If this doesn't makes sense in the context of the kind of API (e.g. selenium),
    then does nothing. Doesn't catch any other exceptions at the moment
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            ret_val = func(self, *args, **kwargs)
            return ret_val
        except (ConnectionAbortedError, pyodbc.OperationalError):
            print('ConnectionAbortedError encountered. Refreshing connection...')
            self.setup()
            ret_val = func(self, *args, **kwargs)
            return ret_val
    return wrapper

class BaseAPIMixin():
    save_filepath=DEFAULT_SAVE_FILEPATH
    def __init__(self, source, **kwargs):
        # This call to super should call DBClassMixin.__init__ with necessary kwargs
        super().__init__(**kwargs)
        self.source=source

    @call_setup
    def run(self, start_date: datetime.date, end_date: datetime.date, *args, **kwargs):
        """
        This function, which is directly called by code outside of the class,
        passes arguments to function `_run` defined by subclasses.

        The return value is stored in self.data and then returned.
        """
        self.start_date = start_date
        self.end_date = end_date

        kwargs['start_date'] = start_date
        kwargs['end_date'] = end_date

        # calls generic method to be implemented by subclass
        # set resulting dataframe as attribute
        try:
            self.data = self._run(*args, **kwargs)
        except Exception as e:
            raise DownloadFailedError from e

        # save data
        self.save()
        return self.data

    def setup(self):
        """
        Fixes problems that are source of ConnectionAbortedError. If not defined,
        calls to setup method of subclasses methods call this method (that does nothing).
        """
        pass

    def _run(self, *args, **kwargs):
        """
        Internal run method to be overridden by subclass.
        """
        raise NotImplementedError

    def save(self):
        if not hasattr(self, 'data') or not isinstance(self.data, pd.DataFrame):
            print(f'Unable to save source {self.source}')
            if not hasattr(self, 'data'):
                print('Nothing to save.')
            elif self.data is not None:
                full_path = self.save_filepath + 'failed_' + self.source + '%s-%s.%s-%s' % (self.start_date.month, self.start_date.month, self.end_date.month, self.end_date.day)
                with open(full_path, 'wb') as file:
                    pickle.dump(self.data, file)
                print('Dumped self.data in pickle file.')
            return

        full_path = self.save_filepath + self.source + '_%s-%s.%s-%s.csv' % (
        self.start_date.month, self.start_date.month, self.end_date.month, self.end_date.day)
        print(f'Saving source {self.source} to {self.save_filepath}')
        self.data.to_csv(full_path)


class APIDBBase(BaseAPIMixin, DBClassMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self, start_date: datetime.date, end_date: datetime.date, do_insert: bool = False):
        if not isinstance(start_date, datetime.date) or not isinstance(end_date, datetime.date):
            raise ValueError(f'start date is {start_date.__class__}, end date is {end_date.__class__}')
        self.run(start_date=start_date, end_date=end_date)
        if do_insert:
            self.insert()

