from mcauto.core.api.base import APIDBBase, DownloadFailedError
from mcauto.config.config import DM_URLS
import requests
import pandas as pd

CAMP_COLS_TO_DROP = ['CampaignAdvertiserCode', 'CampaignAdvertiserName', 'CampaignBudgetCurrencyCode', 'IsBudgetFromAuthorizations', 'LocationName']
PLACE_COLS_TO_DROP = ['SupplierCurrencyCode', 'FeeBillableRate', 'FeeDescription', 'FeeSupplierName','GrossBillable','GrossPayable','PlannedFees','PublisherPaid','SpecialRepCode','ThirdPartyCostSource']
MONTH_COLS_TO_DROP = ['FeeBillableRate', 'FeeDescription', 'FeeSupplierName', 'PayableRate', 'PlannedFeesBil', 'PlannedNetPayable', 'PlannedNetBillable']

class DataMarketAPI(APIDBBase):
    def __init__(self, what, **kwargs):
        super().__init__(**kwargs)

        if what not in DM_URLS:
            raise KeyError(f'{what} not in available urls.')
        self.what = what
        self.get_url = DM_URLS[what]

    def _run(self, *args, **kwargs):
        resp = requests.get(self.get_url)
        if resp.status_code != 200:
            raise DownloadFailedError(f"Download {self.what} failed with status code {resp.status_code}")

        data = resp.json()['Data']

        df = pd.DataFrame.from_records(data)

        return df

class PrismaCampaignDetailsDataMarketAPI(DataMarketAPI):
    def __init__(self):
        super().__init__(what='campaign details',
                         source='PrismaCampaignDetails',
                         table_name='raw_PRISMACampaignDetails',
                         do_truncate=True,
                         drop_columns=CAMP_COLS_TO_DROP)

class PrismaPlacementDetailsDataMarketAPI(DataMarketAPI):
    def __init__(self):
        super().__init__(what='placement details',
                         source='PrismaPlacementDetails',
                         table_name='raw_PRISMAPlacementDetails',
                         do_truncate=True,
                         drop_columns=PLACE_COLS_TO_DROP)

class PrismaMonthlyDeliveryDataMarketAPI(DataMarketAPI):
    def __init__(self):
        super().__init__(what='monthly delivery',
                         source='PrismaMonthlyDelivery',
                         table_name='raw_PRISMAMonthlySpend',
                         do_truncate=True,
                         drop_columns=MONTH_COLS_TO_DROP)

