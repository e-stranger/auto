#
# Copyright 2018 Google LLC (setup code)
#
from httplib2 import ServerNotFoundError

import functools
import io
import os
import time

import httplib2
import pandas as pd
from googleapiclient import http as google_http

from googleapiclient import discovery
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools
import datetime

from mcauto.core.api.base import APIDBBase, DownloadFailedError
from mcauto.core.database.database import DBClassMixin

# Name, scopes, and version for DCM DFA Reporting API
DCM_API_NAME = 'dfareporting'
DCM_API_VERSION = 'v3.3'
DCM_API_SCOPES = ['https://www.googleapis.com/auth/dfareporting']

# Name, scopes, and version for DoubleClick Bid Manager API
DBM_API_NAME = 'doubleclickbidmanager'
DBM_API_VERSION = 'v1'
DBM_API_SCOPES = ['https://www.googleapis.com/auth/doubleclickbidmanager']

# Name, scopes, and version for Google Analytics Reporting API
GA_API_NAME = 'analyticsreporting'
GA_API_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
GA_API_VERSION = 'v4'
GA_DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
GA_VIEW_ID = '156712626'


class AdidasGoogleAPI(APIDBBase):
    """
    Utility class that does the OAuth2 dance using google-api-python-client library.
    This is subclassed by DoubleclickBidManagerAPI, DoubleclickCampaignManagerAPI, and AnalyticsReportingAPI

    """
    def __init__(self, source: str, table_name:str, do_truncate: bool, drop_columns: list, client_secrets: str, api_name: str, api_scopes: list, api_version: str, *args, **kwargs):
        """
        Args:
            source: source name passed to BaseAPIMixin
            table_name: ultimate SQL table destination of data. Passed to BaseAPIMixin.__init__ -> DBClassMixin.__init__
            do_truncate: whether to truncate `table_name` or not. Passed to BaseAPIMixin.__init__ -> DBClassMixin.__init__
            drop_columns: columns to drop from the result of API call. Passed to BaseAPIMixin.__init__ -> DBClassMixin.__init__
            client_secrets: string with filename of client secrets json.
            api_name: Valid API name.
            api_scopes: Valid list of API scopes.
            api_version: Valid API version.
        """

        # call to BaseAPIMixin.__init__(), which itself should call DBClassMixin.__init__
        super().__init__(source=source, table_name=table_name, do_truncate=do_truncate, drop_columns=drop_columns)

        self.client_secrets = client_secrets
        self.api_name = api_name
        self.api_scopes = api_scopes
        self.api_version = api_version
        self.credential_store_file = 'storage/' + api_name + '.dat'
        self.setup()


    def _load_application_default_credentials(self):
        """
        ***GOOGLE DOCUMENTATION***

        Attempts to load application default credentials.
        Returns:
          A credential object initialized with application default credentials or None
          if none were found.
        """
        try:
            credentials = client.GoogleCredentials.get_application_default()
            return credentials.create_scoped(self.api_scopes)
        except client.ApplicationDefaultCredentialsError:
            # No application default credentials, continue to try other options.
            pass

    def _load_user_credentials(self, storage):
        """
        ***GOOGLE DOCUMENTATION***

        Attempts to load user credentials from the provided client secrets file.
        Args:
          client_secrets: path to the file containing client secrets.
          storage: the data store to use for caching credential information.
          flags: command-line flags.
        Returns:
          A credential object initialized with user account credentials.
        """
        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
            self.client_secrets,
            scope=self.api_scopes,
            message=tools.message_if_missing(self.client_secrets))

        # Retrieve credentials from storage.
        # If the credentials don't exist or are invalid run through the installed
        # client flow. The storage object will ensure that if successful the good
        # credentials will get written back to file.

        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage)

        return credentials

    def setup(self):
        """
        ***GOOGLE DOCUMENTATION***

        Handles authentication and loading of the API.
        Args:
          flags: command-line flags obtained by calling ''get_arguments()''.
        Returns:
          An initialized service object.
        """
        # Load application default credentials if they're available.
        self.credentials = self._load_application_default_credentials()

        # Otherwise, load credentials from the provided client secrets file.
        # Name of a file containing the OAuth 2.0 information for this
        # application, including client_id and client_secret, which are found
        # on the Credentials tab on the Google Developers Console.
        self.client_secrets = os.path.join(os.path.dirname(__file__),
                                           self.client_secrets)

        credential_store_file = os.path.join(os.path.dirname(__file__),
                                             self.credential_store_file)

        storage = oauthFile.Storage(credential_store_file)

        if self.credentials is None or self.credentials.invalid:
            self.credentials = self._load_user_credentials(storage)

        # Authorize HTTP object with the prepared credentials.
        http = self.credentials.authorize(http=httplib2.Http())

        # Construct and return a service object via the discovery service.
        self.service = discovery.build(self.api_name, self.api_version, http=http)
        return self.service

class DoubleclickBidManagerAPI(AdidasGoogleAPI):
    def __init__(self, client_secrets='client_secret.json', *args, **kwargs):
        super().__init__(source='DV360', table_name='raw_DV360', do_truncate=False, drop_columns=[], client_secrets=client_secrets, api_name=DBM_API_NAME, api_scopes=DBM_API_SCOPES, api_version=DBM_API_VERSION, *args, **kwargs)

    def _run(self, *args, **kwargs):
        try:
            pass
        except Exception as e:
            raise DownloadFailedError from e

    #def read_csv_from_str(self):
    #    if not hasattr(self, 'str_data'):
    #        raise ValueError
    #    strio = io.StringI

class DoubleclickCampaignManagerAPI(AdidasGoogleAPI):

    def __init__(self, source, table_name, do_truncate, drop_columns, client_secrets: str ='client_secret.json'):
        """
        Args:
            client_secrets: path relative to this file
        """
        super().__init__(source=source,
                         table_name=table_name,
                         do_truncate=do_truncate,
                         drop_columns=drop_columns,
                         client_secrets=client_secrets,
                         api_name=DCM_API_NAME,
                         api_scopes=DCM_API_SCOPES,
                         api_version=DCM_API_VERSION)

        # Get and save profile ID.
        self.get_profile_id()
        # Save current reports
        self.current_reports=self.list_reports()

    def get_profile_id(self):
        """
        Gets and saves profileId needed to access DCM data.
        """
        # get list of user profile
        results = self.service.userProfiles().list().execute()
        # TODO: create custom exceptions
        assert 'items' in results
        # TODO: create custom exceptions
        assert len(results['items'])
        # store profileId
        self.profileId = results['items'][0]['profileId']

        return self.profileId

    def list_reports(self):
        response_reports = self.service.reports().list(profileId=self.profileId).execute()
        return response_reports

    def update_report_date(self, reportId: str, start_date: datetime.date, end_date: datetime.date):
        """

        Args:
            reportId: Previously fetched reportId
            start_date:
            end_date:

        Returns:

        """
        f_start_date = start_date.strftime('%Y-%m-%d')
        f_end_date = end_date.strftime('%Y-%m-%d')
        updated_report = self.service.reports().patch(profileId=self.profileId, reportId=reportId, body={
            'criteria': {'dateRange': {'startDate': f_start_date, 'endDate': f_end_date}}, "FORMAT": "CSV"}).execute()
        return updated_report

    def run_report(self, reportId):
        """
        Runs report given by `reportId` with previously fetched profileId.

        Args:
            str `profileId`

        Returns:
            dict
        """
        print(f'Running report {reportId}...')
        return self.service.reports().run(profileId=self.profileId, reportId=reportId).execute()

    def check_report(self, reportId, fileId):
        seconds_elapsed = 0

        while True:
            report = self.service.files().get(reportId=reportId, fileId=fileId).execute()
            if report['status'] == 'REPORT_AVAILABLE':
                print(f'Report {report["id"]} finished in {seconds_elapsed} seconds.')
                return report
            elif report['status'] == 'PROCESSING':
                time.sleep(2)
                seconds_elapsed += 2
            else:
                print(report['status'])
                return False

    def download_report(self, reportId, fileId, filename, chunksize=64 * 64 * 32 * 8) -> io.FileIO:
        """

        Args:
            reportId: report ID of previously completed report to download
            fileId:   file to download
            filename: name of file to save
            chunksize: size of chunk (in bytes) to download reports.

        Returns:
            io.FileIO

        """
        request = self.service.files().get_media(reportId=reportId, fileId=fileId)

        out_file = io.FileIO(filename, mode='wb+')
        downloader = google_http.MediaIoBaseDownload(out_file, request, chunksize=chunksize)

        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()

        return out_file

    def run_download_report_by_name(self, report_name: str, start_date: datetime.date, end_date: datetime.date, filename='testdl_%s_%s.%s-%s.%s.csv'):
        """

        Args:
            report_name: string of report name
            start_date:  inclusive first day of data to be reported on.
            end_date:    inclusive last day of data to be reported on.
            filename:    path + filename of file to be saved.

        Returns:
            pd.DataFrame with report result

        """
        conv = [(report['id'], report['name']) for report in self.current_reports['items'] if report['name'] == report_name]
        # there should only be one report with that name. Fails if not.
        if len(conv) != 1:
            raise ValueError(f'There are either 0 or more than 1 reports with the requested name {report_name}')

        # save report ID
        reportId = conv[0][0]

        # Update the date
        self.update_report_date(reportId=reportId, start_date=start_date, end_date=end_date)

        # Run the report
        rep = self.run_report(reportId)

        # Wait until report is finished running

        rep = self.check_report(reportId=reportId, fileId=rep['id'])

        filename = self.save_filepath + filename % (report_name, start_date.month, start_date.day, end_date.month, start_date.day)

        # Download and save
        return self.download_to_local_and_load(reportId, fileId=rep['id'], filename=filename)

    def download_to_local_and_load(self, reportId, fileId, filename) -> pd.DataFrame:
        """
        Downloads report, loads it into memory, and returns it.

        Returns:
            pd.DataFrame with report data

        """
        self.download_report(reportId, fileId, filename)
        df = pd.read_csv(filename, skipfooter=1, skiprows=12, engine="python")
        return df

    def _run(self, start_date, end_date, what: str):
        if what.lower() == 'delivery':
            name = 'weekly reporting delivery'
        elif what.lower() == 'conversions':
            name = 'weekly reporting conversions'
        else:
            print(f"{what} not supported.")
            return None
        try:
            df = self.run_download_report_by_name(name, start_date, end_date)
        except Exception as e:
            raise DownloadFailedError from e
        return df

class DoubleclickCampaignManagerDeliveryAPI(DoubleclickCampaignManagerAPI):
    """
    Convenience class to download DCM Delivery data.
    """
    def __init__(self):
        super().__init__(source='DCM_Delivery',
                         table_name='raw_DCMDelivery',
                         do_truncate=False,
                         drop_columns=[])

    def _run(self, start_date: datetime.date, end_date: datetime.date, *args, **kwargs):
        return super()._run(start_date=start_date, end_date=end_date, what='delivery')

class DoubleclickCampaignManagerConversionAPI(DoubleclickCampaignManagerAPI):
    def __init__(self, table_name='raw_DCMConversions'):
        super().__init__(source='DCM_Conversions',
                         table_name=table_name,
                         do_truncate=False,
                         drop_columns=[])
    """
        Convenience class to download DCM Conversions data.
        """
    def _run(self, start_date: datetime.date, end_date: datetime.date, *args, **kwargs):
        return super()._run(start_date=start_date, end_date=end_date, what='conversions')