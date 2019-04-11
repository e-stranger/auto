import pytest
from mcauto.core.database.database import create_database
from mcauto.core.load import get_adidas_analysis

@pytest.fixture(scope="session")
def get_adna():
    return get_adidas_analysis()


@pytest.fixture(scope="module")
def test_adidas_pyodbc():
    return create_database('Adidas', use_sqlalchemy=False)


@pytest.fixture(scope="module")
def test_adidas_sqlalchemy():
    return create_database('Adidas', use_sqlalchemy=True)