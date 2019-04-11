import os

resource_path = '.\\mcauto\\resources'
train_filename = 'training.1600000.processed.noemoticon.csv'
train_path = os.path.join(resource_path, train_filename)

DEFAULT_SAVE_FILEPATH='C:\\Users\\john.atherton\\Documents\\api\\'


pull_urls = {
    "campaign details": "https://data.groupm.com/api/SavedQuery/a3d179fa-57bb-487a-acc7-10b37cf7322f?applicationtype=DataMarketplace_API&useremail=john.atherton%40mediacom.com&applicationtoken=ea55447a-bd17-404c-8539-87ecba7e72d1",
    "monthly delivery": "https://data.groupm.com/api/SavedQuery/514f5d85-c65d-43fc-a7e3-48fa5dc5ab2b?applicationtype=DataMarketplace_API&useremail=john.atherton%40mediacom.com&applicationtoken=ea55447a-bd17-404c-8539-87ecba7e72d1",
    "placement details": "https://data.groupm.com/api/SavedQuery/cca1c7ff-f848-4a99-ba5f-3ea2214ae8b1?applicationtype=DataMarketplace_API&useremail=john.atherton%40mediacom.com&applicationtoken=ea55447a-bd17-404c-8539-87ecba7e72d1"
}

class DataMarketURLDownload():
    _pull_urls = pull_urls


    def __getitem__(self, key):
        if key.lower() not in self._pull_urls:
            raise KeyError(f'{key} not in keys')
        return self._pull_urls[key.lower()]

    def __setitem__(self, key, value):
        if key.lower() not in self._pull_urls:
            self._pull_urls[key.lower()] = value

    def __contains__(self, key):
        return (key.lower() in self._pull_urls)

DM_URLS = DataMarketURLDownload()

my_email = "jchaunceya@gmail.com"
my_password = "$eiko009"