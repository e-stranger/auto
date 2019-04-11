import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
from mcauto.core.database.database import create_database
import openpyxl as oxl
import win32com.client as win32

# If modifying these scopes, delete the file token.pickle.
#SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '11hW6CZfbN_938idCMQ17BbPb13Cqr32RanbhqIiAJUk'
SOCIAL_RANGE = 'Social'
SD_RANGE = 'Site Direct'
PROG_RANGE = 'Programmatic'

class SheetsUtils:

    def setup(self):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'C:\\Users\\john.atherton\\Downloads\\credentials (1).json', SCOPES)
                creds = flow.run_local_server()
            # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

        self.service = build('sheets', 'v4', credentials=creds)
        return self.service

    def __init__(self):
        self.setup()

class SheetsExclusionAutomaton(SheetsUtils):
    def __init__(self):
        super().__init__()



def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'C:\\Users\\john.atherton\\Downloads\\credentials (1).json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    return service
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        print('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            print('%s, %s' % (row[0], row[4]))

sql = """select		[Exclusion Type],
			[Raw_Campaign Name],
			isnull([PRI_Campaign Name],'')[PRI_Campaign Name],
			[Raw_Site Name],
			[Raw_Placement ID],
			[Raw_Placement Name],
			isnull([PRI_Placement Id],'')[PRI_Placement Id],
			isnull([PRI_Placement Name],'')[PRI_Placement Name],
			sum(cast(isnull([Final Delivered Media Cost], 0) as numeric (18,4)))[Delivered Spend],
			sum(cast(isnull([Impressions],0) as numeric (18,4))) [Impressions],
			sum(cast(isnull([Clicks],0) as numeric (18,4)))[Clicks],
			sum(cast(isnull([Click-through Conversions],0) as numeric (18,4)))[Click-through Conversions],
			sum(cast(isnull([View-through Conversions],0) as numeric (18,4)))[View-through Conversions],
			sum(cast(isnull([Click-Through Revenue],0) as numeric (18,4)))[Click-Through Revenue],
			sum(cast(isnull([View-through Revenue],0) as numeric (18,4)))[View-through Revenue]
from		[dbo].[Final Table (exclusions)]
where		[Data Source Association] <> 'GA 360' and [Raw_Date] > '2019-03-30'
group by	[Exclusion Type],
			[Raw_Campaign Name],
			[PRI_Campaign Name],
			[Raw_Site Name],
			[Raw_Placement ID],
			[Raw_Placement Name],
			[PRI_Placement Id],
			[PRI_Placement Name]
order by	[Exclusion Type],
			[Raw_Campaign Name],
			[Raw_Site Name]
"""

f1 = """=IF(COUNTIF('Status Mapping'!E:E,'Data Input'!D%s),VLOOKUP(D%s,'Status Mapping'!A:C,3,FALSE),"New Value")"""
f2 = """=H%s&J%s&K%s&L%s"""
f3 = """=IF(AND(ISNUMBER(SEARCH("eCom",H%s)),ISNUMBER(SEARCH("PaidSocial",H%s))),"Social eCom",IF(AND(ISNUMBER(SEARCH("Brand",H%s)),ISNUMBER(SEARCH("PaidSocial",H%s))),"Social Brand",IF(AND(ISNUMBER(SEARCH("Brand",H%s)),ISNUMBER(SEARCH("Snap",F%s))),"Social Brand",IF(AND(ISNUMBER(SEARCH("Brand",H%s)),ISNUMBER(SEARCH("Pinterest",F%s))),"Social Brand",IF(AND(ISNUMBER(SEARCH("Brand",H%s)),ISNUMBER(SEARCH("Twitter",F%s))),"Social Brand",IF(AND(ISNUMBER(SEARCH("Brand",H%s)),ISNUMBER(SEARCH("Facebook",F%s))),"Social Brand",IF(AND(ISNUMBER(SEARCH("Brand",H%s)),ISNUMBER(SEARCH("Instagram",F%s))),"Social Brand",VLOOKUP(F%s,'Team Assignment Mapping'!A:B,2,FALSE))))))))"""
f4 = """=VLOOKUP(J%s,'Clean Site Mapping'!A:B,2,FALSE)"""

fname = 'C:\\Users\\john.atherton\\Documents\\exclusions_template2.xlsx'

db = create_database('Adidas', use_sqlalchemy=False, do_connect=True)
data_pull = db.execute(sql)

data_pull = data_pull.fillna('')
#TODO assert data_pull.shape[1] == 14
values = []

for i in range(data_pull.shape[0]):
    row = [str(x) for x in data_pull.iloc[i].tolist()]
    values.append(row)

wb = oxl.load_workbook(fname)
data_input = wb['Data Input']


ctr = 1
while True:
    if data_input[f'E{ctr}'].value is not None:
        ctr += 1
    else:
        break

for i in range(len(values)):
    row = i + ctr
    data_input[f'B{row}'].value = f1 % (row, row)
    data_input[f'D{row}'].value = f2 % (row, row, row, row)
    data_input[f'E{row}'].value = f3 % (row, row, row, row, row, row, row, row, row, row, row, row, row, row, row)
    data_input[f'F{row}'].value = f4 % (row)
    for j,letter in enumerate(['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']):
        cell = letter + str(row)
        data_input[cell].value = values[i][j]

wb.save(fname)
wb.close()

first = ctr
last = ctr + len(values) - 1

cols = ['A','B', 'C', 'D', 'E', 'F','G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

excel = win32.Dispatch('Excel.Application')
wb = excel.Workbooks.Open(fname)
ws = wb.Worksheets(1)


evaluated_vals = []
try:
    header = []
    for letter in cols:
        val = ws.Cells(1, cols.index(letter) + 1).value
        header.append(val)
    for i in range(first, last+1):
        evaluated_val = []
        for letter in cols:
            val = ws.Cells(i, cols.index(letter)+1).value
            evaluated_val.append(val)
        evaluated_vals.append(evaluated_val)

except:
    pass

evaluated_df = pd.DataFrame(evaluated_vals, columns=header)

#finally:
#wb = pd.read_excel('exclusions_template.xlsx', 'Data Input')