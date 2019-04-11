from mcauto.core.api.data_market import PrismaCampaignDetailsDataMarketAPI, PrismaMonthlyDeliveryDataMarketAPI, PrismaPlacementDetailsDataMarketAPI
from mcauto.core.database.database import SQLAlchemyUtils

class PrismaCampaignDetailsHandler(PrismaMonthlyDeliveryDataMarketAPI):
    def __init__(self):
        super().__init__()
        data = self.run()

class DatabaseAPIMixin():
    def __init__(self):
        self.sql = SQLAlchemyUtils()


    def insert(self, table_name, do_truncate):
        if not hasattr(self, 'data'):
            raise ValueError('self.data must exist')
        self.sql.insert_clean_df(self.data, table_name, if_exists='append', do_truncate=do_truncate)
        pass
