#
# Copyright 2018 Google LLC

import argparse
import functools
import io
import os
import time

import httplib2
import pandas as pd
from googleapiclient import http as google_http

DBM_API_NAME = 'doubleclickbidmanager'
DBM_API_VERSION = 'v1'
DBM_API_SCOPES = ['https://www.googleapis.com/auth/doubleclickbidmanager']

GA_NAME = 'analyticsreporting'
GA_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
GA_VERSION = 'v4'

# Filename used for the credential store.
DBM_CREDENTIAL_STORE_FILE = DBM_API_NAME + '.dat'


def get_arguments(argv, desc, parents=None):
    """Validates and parses command line arguments.
    Args:
      argv: list of strings, the command-line parameters of the application.
      desc: string, a description of the sample being executed.
      parents: list of argparse.ArgumentParser, additional command-line parsers.
    Returns:
      The parsed command-line arguments.
    """
    # Include the default oauth2client argparser
    parent_parsers = [tools.argparser]

    if parents:
        parent_parsers.extend(parents)

    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=parent_parsers)
    return parser.parse_args(argv[1:])


def call_setup(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except ConnectionAbortedError:
            print('ConnectionAbortedError encountered. Refreshing connection...')
            self.refresh_token()
            return func(self, *args, **kwargs)

    return wrapper


class AdidasGoogleAPI:
    def __init__(self, client_secrets, api_name, api_scopes, api_version):
        self.client_secrets = client_secrets
        self.api_name = api_name
        self.api_scopes = api_scopes
        self.api_version = api_version
        self.credential_store_file = api_name + '.dat'
        self.setup()
        self.get_profile_id()

    def _load_application_default_credentials(self):
        """Atempts to load application default credentials.
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
        """Attempts to load user credentials from the provided client secrets file.
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
        """Handles authentication and loading of the API.
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
        self.service = discovery.build(API_NAME, API_VERSION, http=http)
        return self.service

    def refresh_token(self):
        http = self.credentials.refresh(http=httplib2.Http())

        # Authorize HTTP object with the prepared credentials.
        http = self.credentials.authorize(http=httplib2.Http())
        self.service = discovery.build(self.api_name, self.api_version, http=http)

    def get_profile_id(self):
        # get list of user profile
        results = self.service.userProfiles().list().execute()
        # TODO: create custom exceptions
        assert 'items' in results
        # TODO: create custom exceptions
        assert len(results['items'])
        # store profileId
        self.profileId = results['items'][0]['profileId']

        return self.profileId

    @call_setup
    def list_reports(self):
        response_reports = self.service.reports().list(profileId=self.profileId).execute()
        return response_reports

    @call_setup
    def update_report_date(self, reportId, start_date, end_date):
        f_start_date = start_date.strftime('%Y-%m-%d')
        f_end_date = end_date.strftime('%Y-%m-%d')
        updated_report = self.service.reports().patch(profileId=self.profileId, reportId=reportId, body={
            'criteria': {'dateRange': {'startDate': f_start_date, 'endDate': f_end_date}}, "FORMAT": "CSV"}).execute()
        return updated_report

    @call_setup
    def run_report(self, reportId):
        """
        Runs report given by `reportId` with previously fetched profileId.

        Args:
            str `profileId`

        Returns:
            dict
        """
        return self.service.reports().run(profileId=self.profileId, reportId=reportId).execute()

    @call_setup
    def check_report(self, reportId, fileId):
        seconds_elapsed = 0

        while True:
            report = self.service.files().get(reportId=reportId, fileId=fileId).execute()
            if report['status'] == 'REPORT_AVAILABLE':
                print(f'Report {report["id"]} finished in {seconds_elapsed} seconds.')
                return report
            elif report['status'] == 'PROCESSING':
                time.sleep(5)
                seconds_elapsed += 5
            else:
                print(report['status'])
                return False

    @call_setup
    def download_report(self, reportId, fileId, filename):
        request = self.service.files().get_media(reportId=reportId, fileId=fileId)

        out_file = io.FileIO(filename, mode='wb')
        downloader = google_http.MediaIoBaseDownload(out_file, request, chunksize=64 * 64 * 32 * 4)

        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()


class AdidasGoogleAPIHandler(AdidasGoogleAPI):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def download_to_local_and_load(self, reportId, fileId, filename):
        self.download_report(reportId, fileId, filename)
        df = pd.read_csv(filename, skipfooter=1, skiprows=12, engine="python")
        return df


from googleapiclient import discovery
from oauth2client import client
from oauth2client import file as oauthFile
from oauth2client import tools

API_NAME = 'dfareporting'
API_VERSION = 'v3.3'
API_SCOPES = ['https://www.googleapis.com/auth/dfareporting',
              'https://www.googleapis.com/auth/dfatrafficking',
              'https://www.googleapis.com/auth/ddmconversions']
