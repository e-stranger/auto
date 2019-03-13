import datetime

import numpy as np
import pandas as pd
import requests
import win32com.client as win32

import mcauto.core.database
import mcauto.core.excel
from mcauto.config import pull_urls

PROGRAMMATIC_BENCHMARK = 2
SOCIAL_BENCHMARK = 2
DIRECT_BENCHMARK = 2

DATE_FORMAT = '%Y-%m-%d'

# Base class that provides date formatting and calculation

"""
    Provides basic date calculation and formatting for child classes.
     
    `date_format`: string to parse and format date.
    `begin_date`:  datetime.date object with inclusive beginning of reporting period.
    `end_date`:    datetime.date object with inclusive ending of reporting period.
    `last_begin_date`: datetime.date object with inclusive beginning of previous reporting period.
    `last_end_date`: datetime.date object with inclusive ending of previous reporting period.
"""


class BaseWeeklyReporting():
    def __init__(self, begin_date=None, end_date=None, last_week=False,
                 sun_sat_reporting=False):  # date is in format 'YYYY-MM-DD'

        global DATE_FORMAT

        # set date format to global date format
        self.date_format = DATE_FORMAT

        # if begin and end date have been provided
        if begin_date and end_date:
            self.begin_date = datetime.datetime.strptime(self.begin_date, self.date_format)
            self.end_date = datetime.datetime.strptime(self.end_date, self.date_format)

        else:
            # Previous Monday to Previous Sunday, inclusive
            today = datetime.date.today()
            if last_week:
                today = today - datetime.timedelta(weeks=1)
            if not sun_sat_reporting:
                self.begin_date = today - datetime.timedelta(days=today.weekday(), weeks=1)
                self.end_date = today - datetime.timedelta(days=today.weekday() + 1)
            else:
                self.begin_date = today - datetime.timedelta(days=today.weekday() + 1, weeks=1)
                self.end_date = today - datetime.timedelta(days=today.weekday() + 2)

        tmp_exclusive_begin = self.begin_date - datetime.timedelta(days=1)

        self.since_date_str = '%s-%s-%s' % (
            tmp_exclusive_begin.year, tmp_exclusive_begin.month, tmp_exclusive_begin.day)

        self.last_begin_date = self.begin_date - datetime.timedelta(days=7)
        self.last_end_date = self.end_date - datetime.timedelta(days=7)

    def get_to_match(self, limit_date, path_filepath_format, return_df_dict):
        raise NotImplementedError('Subclass')

    def match(self):
        raise NotImplementedError('Subclass')


"""
Class which adds SQL Server database access to date formatting class BaseWeeklyReporting
`get_database_fn`: generic function with no arguments 
`db`: instance of mcauto.database.AdidasDatabase
"""


class AdidasWeeklyReporting(BaseWeeklyReporting):
    def __init__(self, get_database_fn=lambda: mcauto.core.database.create_database(account='Adidas', do_connect=True),
                 **kwargs):
        print(kwargs)
        super().__init__(**kwargs)
        self.get_database_fn = get_database_fn
        self.db = self.get_database_fn()


"""
Class which layers analysis functionality on top of date and database functionality
"""


class ADNAnalysis(AdidasWeeklyReporting):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # Helper functions
    def drop_and_rearrange_columns(self, df):
        """
        Drops columns in pd.DataFrame, rearranges remaining

        Args:
            pd.DataFrame `df`
        Returns:
            pd.DataFrame `df`
        """
        cols = [
            'Flag',
            'ROI',
            '% Spend',
            'Placement Name',
            'Prisma ID',
            'PLA_Site',
            'Campaign Name',
            'Campaign ID',
            'Channel',
            'Weekly Adj. Revenue',
            'Weekly Spend',
            'Overall Adj. Revenue',
            'Overall Spend',

            'Overall ROI',
            'Above median spend']

        df = df.drop(
            columns=['Weekly CTRev', 'Weekly Adjusted VTRev', 'Overall CTRev', 'Overall Adjusted VTRev', 'Division',
                     'CAM_Channel', 'Weekly Capped Spend', 'Weekly Partner Reported Spend', 'Overall Capped Spend',
                     'Overall Partner Reported Spend'])
        df = df[cols]

        return df

    def get_current_placements(self, who):
        """
        Calls `Get_Current_Placements` stored procedure with begin, end dates and channel.

        Args:
            str `who`
        Returns:
            pd.DataFrame result set
        """
        return self.db.execute_procedure(
            "Get_Current_Placements '%s', '%s', '%s'" % (self.begin_date, self.end_date, who))

    def flag_placements(self, who):
        """
        Calculates % spend, ROI, adds flags to data returned by `self.get_current_placements`

        Args:
            str `who`

        Returns:
            pd.DataFrame, non-zero spend indicies tuple
        """

        df = self.get_current_placements(who)

        # raise ValueError if not social or programmatic
        if who.lower() == 'social':
            who = 'Social'
            benchmark = SOCIAL_BENCHMARK
        elif who.lower() == 'programmatic':
            who = 'Programmatic'
            benchmark = PROGRAMMATIC_BENCHMARK
        else:
            raise ValueError('Acceptable values of `who` are "Social" and "Programmatic"')

        df['% Spend'] = 100 * df['Weekly Spend'] / df['Weekly Spend'].sum()
        df['ROI'] = (df['Weekly Adj. Revenue'] - df['Weekly Spend']) / df['Weekly Spend']
        df['Overall ROI'] = (df['Overall Adj. Revenue'] - df['Overall Spend']) / df['Overall Spend']

        # Standardize missing values to np.nan
        df = df.replace([np.inf, -np.inf], np.nan)

        # Initialize flag to empty string
        df['Flag'] = ''

        # Flag programmatic ROI under PROGRAMMATIC_BENCHMARK
        df.loc[(df['Channel'] == who) & (~np.isnan(df['ROI'])) & (
                df['ROI'] < benchmark), 'Flag'] = f'{who} ROI under benchmark'
        # Highlist programmatic over PROGRAMMATIC_BENCHMARK
        df.loc[(df['Channel'] == who) & (~np.isnan(df['ROI'])) & (
                df['ROI'] >= benchmark), 'Flag'] = f'{who} ROI exceeds benchmark'

        # Flag placements (not prog) with no revenue

        df.loc[np.isnan(df['ROI']), 'Flag'] = 'No spend for this week'

        # Initialize median spend flag to False
        df['Above median spend'] = False

        # Get non-zero weekly spend indicies
        idx = (df['Weekly Spend'] != 0)

        # Get median values of spend
        med = df['Weekly Spend'].median()

        df.loc[idx, 'Above median spend'] = df['Weekly Spend'].apply(lambda x: x > med)

        return df, idx

    def summarize_campaigns_channel(self, channel, proc='Summarize_Week_Campaigns'):
        sql = f"{proc}  @channel = '%s', @startDate1 = '%s', @endDate1 = '%s', @startDate2 = '%s', @endDate2 = '%s'" % (
            channel,
            self.last_begin_date.strftime(DATE_FORMAT),
            self.last_end_date.strftime(DATE_FORMAT),
            self.begin_date.strftime(DATE_FORMAT),
            self.end_date.strftime(DATE_FORMAT)
        )

        return self.db.execute_procedure(sql)

    def summarize_campaigns_no_channel(self, proc='Summarize_Week_Campaigns'):
        sql = f"{proc} @startDate1 = '%s', @endDate1 = '%s', @startDate2 = '%s', @endDate2 = '%s'" % (
            self.last_begin_date.strftime(DATE_FORMAT),
            self.last_end_date.strftime(DATE_FORMAT),
            self.begin_date.strftime(DATE_FORMAT),
            self.end_date.strftime(DATE_FORMAT))

        return self.db.execute_procedure(sql)

    def summarize_tactic_site_channel(self, channel, proc='Summarize_Week_Tactic_Site_Channel'):
        sql = f"{proc} @channel = '%s', @startDate1 = '%s', @endDate1 = '%s', @startDate2 = '%s', @endDate2 = '%s'" % (
            channel,
            self.last_begin_date.strftime(DATE_FORMAT),
            self.last_end_date.strftime(DATE_FORMAT),
            self.begin_date.strftime(DATE_FORMAT),
            self.end_date.strftime(DATE_FORMAT))
        return self.db.execute_procedure(sql)

    def summarize_tactic_site_no_channel(self, proc='Summarize_Week_Tactic_Site'):
        sql = f"{proc} @startDate1 = '%s', @endDate1 = '%s', @startDate2 = '%s', @endDate2 = '%s'" % (
            self.last_begin_date.strftime(DATE_FORMAT),
            self.last_end_date.strftime(DATE_FORMAT),
            self.begin_date.strftime(DATE_FORMAT),
            self.end_date.strftime(DATE_FORMAT))
        return self.db.execute_procedure(sql)

    def summarize(self, what, channel=None):
        if what == 'Campaigns' and channel:
            return self.summarize_campaigns_channel(channel=channel)
        elif what == 'Campaigns' and channel is None:
            return self.summarize_campaigns_no_channel()
        elif what == 'Tactic x Site' and channel:
            return self.summarize_tactic_site_channel(channel)
        elif what == 'Tactic x Site' and not channel:
            return self.summarize_tactic_site_no_channel()
        else:
            print(
                'Provide valid `what` and `channel` argument: current valid options for `what` are ["Campaigns"], and for `channel`, ["Programmatic", "Social", None].')

    def flag_placements_and_save(self, who, save_filepath='C:\\Users\\john.atherton\\Documents\\Procedures',
                                 filename='%s_triggers%s.%s-%s.%s.xlsx'):
        df, idx = self.flag_placements(who)
        df = self.drop_and_rearrange_columns(df)
        df.sort_values(by='ROI', ascending=False, inplace=True)
        df_channel = df.loc[idx & df['Above median spend']]

        if who.lower() == 'social':
            benchmark = SOCIAL_BENCHMARK
        else:
            benchmark = PROGRAMMATIC_BENCHMARK

        n_below = df[idx]['ROI'].apply(lambda x: int(x < benchmark)).sum()

        campaign_df = self.summarize(what='Campaigns', channel=who)

        tactic_site = self.summarize(what='Tactic x Site', channel=who)

        filename = filename % (who, self.begin_date.month, self.begin_date.day, self.end_date.month, self.end_date.day)

        ew = pd.ExcelWriter(save_filepath + '\\' + filename)

        with ew:

            campaign_df.to_excel(ew, 'Campaign Summary', index=False)
            tactic_site.to_excel(ew, 'Tactic x Site', index=False)

            df.loc[idx].sort_values(by=['ROI', '% Spend'], ascending=True).to_excel(ew, who, index=False)
            # df.loc[df['Channel'] == 'Direct buy'].to_excel(ew, 'Direct buy', index=False)

            if n_below > 100:
                n_below = 100

            df_channel.iloc[-1 * n_below:].to_excel(ew, f'{who} Bottom {n_below}', index=False)
            df_channel.iloc[:5].to_excel(ew, f'{who} Top 5', index=False)

        mcauto.core.excel.autofit_columns(save_filepath + '\\' + filename)

        return df, save_filepath + '\\' + filename

    def flag_save_send(self, who):
        if who.lower() == 'social':

            emails = ['john.atherton@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com', 'AdiSocialPerformance@mediacom.com']
        elif who.lower() == 'programmatic':
            emails = ['john.atherton@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com']
            # emails = ['isabel.czarnecki@mediacom.com', 'jasmine.yeejoybland@mediacom.com', 'moses.galvez@mediacom.com', 'adidas.MediaCom.programmatic@mediacom.com']
        else:
            print('`who` must be either social or programmatic!')
            raise ValueError

        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = "; ".join(emails)
        date_string = '%s-%s-%s / %s-%s-%s' % (
            self.begin_date.month, self.begin_date.day, self.begin_date.year, self.end_date.month, self.end_date.day,
            self.end_date.year)
        mail.Subject = f'AUTOMATED: {who} Triggers {date_string}'
        mail.Body = f'Hello,\nPlease find attached triggers for {who} {date_string}. \nNote: this is an automated email - however, you may respond and I will receive it. \n\nBest,\nJack'

        _, filepath = self.flag_placements_and_save(who=who)

        mail.Attachments.Add(filepath)

        mail.Send()

    def pull_final_table(self, save_filepath=None, begin_date=None, end_date=None):
        if begin_date and not end_date:
            ft = self.db.execute("SELECT * FROM [Final Table] where [Raw_Date] >= '%s'" % begin_date)
        elif not begin_date and end_date:
            ft = self.db.execute("SELECT * FROM [Final Table] where [Raw_Date] <= '%s'" % end_date)
        elif begin_date and end_date:
            ft = self.db.execute(
                "SELECT * FROM [Final Table] where [Raw_Date] >= '%s' and [Raw_Date] <= '%s'" % (begin_date, end_date))
        else:
            ft = self.db.execute("SELECT * FROM [Final Table]")
        if save_filepath:
            ft.to_csv(save_filepath)
        return ft

    def pull_query(self, what="Placement details"):
        if what in pull_urls:
            print(f'Pulling {what}...')
            resp = requests.get(pull_urls[what])
            print(f'Request returned with status code {resp.status_code}')
            if resp.status_code != 200:
                print('Non 200 status code returned. Exiting function...')
                return None
            data = resp.json()['Data']
            return pd.DataFrame(data)
        else:
            print(f'Method not defined for {what}')
            print('Available values: ', ', '.join(pull_urls.keys()))


def get_adidas_analysis(**kwargs):
    return ADNAnalysis(**kwargs)
