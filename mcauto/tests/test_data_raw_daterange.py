from mcauto.core.process.process import DatabaseDateQATask
import datetime

start_date = datetime.date(year=2018, month=9, day=9)
dv360_start_date = datetime.date(year=2018, month=10, day=1)

# With scope="module": 29.57 seconds
# Without: 37 seconds

def test_raw_dcmdelivery_date(get_adna, test_adidas_sqlalchemy):
    DatabaseDateQATask(start_date=start_date, end_date=get_adna.end_date, name='DCMDelivery',
                       proc='MinMaxDateRawQA', engine=test_adidas_sqlalchemy).check()

def test_raw_dcmconversions_date(get_adna, test_adidas_sqlalchemy):
    DatabaseDateQATask(start_date=start_date, end_date=get_adna.end_date, name='DCMConversions',
                       proc='MinMaxDateRawQA', engine=test_adidas_sqlalchemy).check()

def test_raw_dv360_date(get_adna, test_adidas_sqlalchemy):
    DatabaseDateQATask(start_date=dv360_start_date, end_date=get_adna.end_date, name='DV360',
                       proc='MinMaxDateRawQA', engine=test_adidas_sqlalchemy).check()

def test_raw_ga360_date(get_adna, test_adidas_sqlalchemy):
    DatabaseDateQATask(start_date=start_date, end_date=get_adna.end_date, name='GA360',
                       proc='MinMaxDateRawQA', engine=test_adidas_sqlalchemy).check()

def test_raw_facebook_date(get_adna, test_adidas_sqlalchemy):
    DatabaseDateQATask(start_date=start_date, end_date=get_adna.end_date, name='Facebook',
                       proc='MinMaxDateRawQA', engine=test_adidas_sqlalchemy).check()

def test_raw_twitter_date(get_adna, test_adidas_sqlalchemy):
    DatabaseDateQATask(start_date=start_date, end_date=get_adna.end_date, name='Twitter',
                       proc='MinMaxDateRawQA', engine=test_adidas_sqlalchemy).check()
