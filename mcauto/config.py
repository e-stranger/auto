import os

resource_path = '.\\mcauto\\resources'
train_filename = 'training.1600000.processed.noemoticon.csv'
train_path = os.path.join(resource_path, train_filename)

pull_urls = {
    "Campaign details": "https://data.groupm.com/api/SavedQuery/8licationtype=DataMarketplace_API&useremail=john.atherton%40mediacom.com&applicationtoken=ea55447a-bd17-404c-8539-87ecba7e72d1",
    "Monthly delivery": "https://data.groupm.com/api/SavedQuery/514f5d85-c65d-43fc-a7e3-48fa5dc5ab2b?applicationtype=DataMarketplace_API&useremail=john.atherton%40mediacom.com&applicationtoken=ea55447a-bd17-404c-8539-87ecba7e72d1",
    "Placement details": "https://data.groupm.com/api/SavedQuery/cca1c7ff-f848-4a99-ba5f-3ea2214ae8b1?applicationtype=DataMarketplace_API&useremail=john.atherton%40mediacom.com&applicationtoken=ea55447a-bd17-404c-8539-87ecba7e72d1"
}
