from mcauto.core.database.database import create_database
from mcauto.core.process.process import DatabaseDateQATask
import pytest
import pandas as pd
from mcauto.core.load import get_adidas_analysis

from sqlalchemy import Column, Integer, String

def test_database_access(test_adidas_sqlalchemy):
    test_adidas_sqlalchemy.connect()


def test_create_rights(test_adidas_sqlalchemy):
    pass
