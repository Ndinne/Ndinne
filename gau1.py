import requests
from bs4 import BeautifulSoup
response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-1?pagenumber=1&sortorder=Newest&priceoption=RetailPrice")
home_page = BeautifulSoup(response.content, 'lxml')
print(home_page)
