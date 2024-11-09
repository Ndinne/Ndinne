import aiohttp
import asyncio
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import requests
from azure.storage.blob import BlobClient
from datetime import datetime, timedelta

def get_last_page():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/lastpage/last_page.csv?sv=2023-01-03&st=2024-11-09T12%3A38%3A33Z&se=2025-01-09T12%3A38%3A00Z&sr=c&sp=rawdl&sig=1vipjr7uZyYC8KYzv5viMRwgVlj7f9PkejeF7MKaXuo%3D"
    client = BlobClient.from_blob_url(sas_url)

    with open("last_page.csv", "wb") as f:
        data = client.download_blob()
        data_last_page = pd.read_csv(data)

    if len(data_last_page)==0:
        last_page=4080
    else:    
        last_page = data_last_page.iloc[0, 0]

    return last_page

def run_number_retry():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/run/run_number_retry.csv?sv=2023-01-03&st=2024-11-09T12%3A40%3A19Z&se=2025-01-10T12%3A40%3A00Z&sr=c&sp=racwdl&sig=%2Fxu30BxMkElHgeUln1U2nsAA1lKmisegS132qLonoMs%3D"

    client = BlobClient.from_blob_url(sas_url)

    with open("run_number_retry.csv", "wb") as f:
        data = client.download_blob()
        data_run = pd.read_csv(data)

    if len(data_run) == 0:
        run_retry = 0
    else:
        run_retry = data_run.iloc[0, 0]

    return run_retry


def run_number():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/run/run_number.csv?sv=2023-01-03&st=2024-11-09T12%3A40%3A19Z&se=2025-01-10T12%3A40%3A00Z&sr=c&sp=racwdl&sig=%2Fxu30BxMkElHgeUln1U2nsAA1lKmisegS132qLonoMs%3D"

    client = BlobClient.from_blob_url(sas_url)

    with open("run_number.csv", "wb") as f:
        data = client.download_blob()
        data_run = pd.read_csv(data)

    if len(data_run) == 0:
        run = 0
    else:
        run = data_run.iloc[0, 0]

    return run

def reset_run():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/run/run_number.csv?sv=2021-10-04&st=2023-11-03T14%3A19%3A39Z&se=2050-11-04T14%3A19%3A00Z&sr=c&sp=racwdxltf&sig=vfN2PRe1%2BzQGdI2roDDgL12AGS4Vehu7my8K4FoVngk%3D"
    run_number_ = [0]

    pd_run = pd.DataFrame(run_number_)

    run_csv = pd_run.to_csv(index = False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(run_csv, overwrite=True)

def reset_run_retry():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/run/run_number_retry.csv?sv=2021-10-04&st=2023-11-03T14%3A19%3A39Z&se=2050-11-04T14%3A19%3A00Z&sr=c&sp=racwdxltf&sig=vfN2PRe1%2BzQGdI2roDDgL12AGS4Vehu7my8K4FoVngk%3D"
    run_number_retry = [0]

    pd_run_retry = pd.DataFrame(run_number_retry)

    run_csv_retry = pd_run_retry.to_csv(index = False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(run_csv_retry, overwrite=True)

def reset_page():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/page/page.csv?sv=2023-01-03&st=2024-11-09T12%3A41%3A49Z&se=2024-12-31T12%3A41%3A00Z&sr=c&sp=racwdl&sig=wqW5hc7il9v46zDGDJembwEBe%2F7wIha7D%2BL7uXfHZPo%3D"
    last_page = [100]

    pd_page = pd.DataFrame(last_page)

    page_csv = pd_page.to_csv(index=False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(page_csv, overwrite=True)

def new_page_number():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/page/page.csv?sv=2023-01-03&st=2024-11-09T12%3A41%3A49Z&se=2024-12-31T12%3A41%3A00Z&sr=c&sp=racwdl&sig=wqW5hc7il9v46zDGDJembwEBe%2F7wIha7D%2BL7uXfHZPo%3D"
    
    last_page = [(get_last_scraped_page()-11) + 100]

    pd_page = pd.DataFrame(last_page)

    page_csv = pd_page.to_csv(index=False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(page_csv, overwrite=True)

def new_run_number():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/run/run_number.csv?sv=2021-10-04&st=2023-11-03T14%3A19%3A39Z&se=2050-11-04T14%3A19%3A00Z&sr=c&sp=racwdxltf&sig=vfN2PRe1%2BzQGdI2roDDgL12AGS4Vehu7my8K4FoVngk%3D"
    
    run_number_1 = run_number() + 1

    run_number_ = []

    run_number_.append(run_number_1)

    pd_run = pd.DataFrame(run_number_)

    run_csv = pd_run.to_csv(index=False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(run_csv, overwrite=True)

def new_run_number_retry():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/run/run_number_retry.csv?sv=2023-01-03&st=2024-11-09T12%3A40%3A19Z&se=2025-01-10T12%3A40%3A00Z&sr=c&sp=racwdl&sig=%2Fxu30BxMkElHgeUln1U2nsAA1lKmisegS132qLonoMs%3D"
    
    run_number_1 = run_number_retry() + 1

    run_number_ = []

    run_number_.append(run_number_1)

    pd_run = pd.DataFrame(run_number_)

    run_csv = pd_run.to_csv(index=False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(run_csv, overwrite=True)

def get_last_scraped_page():
    sas_url= f"https://autotraderstorage.blob.core.windows.net/page/page.csv?sv=2023-01-03&st=2024-11-09T12%3A41%3A49Z&se=2024-12-31T12%3A41%3A00Z&sr=c&sp=racwdl&sig=wqW5hc7il9v46zDGDJembwEBe%2F7wIha7D%2BL7uXfHZPo%3D"
    
    client = BlobClient.from_blob_url(sas_url)

    with open("page.csv", "wb") as f:
        data = client.download_blob()
        data1 = pd.read_csv(data)
        
        if len(data1)==0:
            page=100
        else:
            page = data1.iloc[0, 0]

    return page



start_page = get_last_scraped_page() - 99

last_page = get_last_scraped_page()

limit = get_last_page()
tasks = []
tasks_2 = []
thread_data = []
base_url = "https://www.autotrader.co.za"
provinces = []
found_link_test = []
async def scrape_page(session, url, page):
    async with session.get(url) as response:
        print(response.status)
        if response.status == 200:
            pass
        else:
            async with session.get(url) as response:
                print(response.status)
                return response

        if response.status == 200:
            page_content = await response.text()
            home_page = BeautifulSoup(page_content, 'html.parser')
            #print(home_page)

            # Find all the car listings on the page 
            import re
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile__NSAT4E2EbrD8w7kA .*')})
            #print(cars_containers)
            for each_div in cars_containers:         

                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    #print(link)
                    try:
                        if '?' not in link['href']:
                            pass
                        else:
                            found_link = (base_url + link['href'])
                            #print(found_link)
                            test_values = found_link, f'page = {page}'
                            found_link_test.append(test_values)    
                            Car_ID = re.search(r'/(\d+)\?', found_link).group(1) 
                            
                            title = each_div.find('span', class_='e-title__PWADYWpQJlv5U7Pv').text.strip()  if each_div.find('span', class_='e-title__PWADYWpQJlv5U7Pv') else None 
                            price_span = each_div.find('span', class_='e-price__fz79voUOfPnB65Lt')
                            if price_span:
                                inner_span = price_span.find('span')
                                if inner_span:
                                    price = inner_span.text.strip()
                                else:
                                    price = price_span.text.strip()
                            else:
                                price = None
                            expected_payment = each_div.find('span', class_='e-estimated-payment__GlVGt6ufpHuXnalm').text.strip() if each_div.find('span', class_='e-estimated-payment__GlVGt6ufpHuXnalm') else None  
                            current_datetime1 = datetime.now()
                            # Add 2 hours to the current datetime
                            new_datetime = current_datetime1 + timedelta(hours=2)
                            current_datetime = new_datetime.strftime('%Y-%m-%d')
                            #current_datetime = datetime.datetime.now().strftime('%Y-%m-%d')
                            
                            # Check if car type is listed under a specific list item
                            li_car_type = each_div.find('li', class_='e-summary-icon m-type')
                            if li_car_type:
                                car_type = li_car_type.text.strip()

                            # Check if car type is listed within the specified unordered list
                            ul_car_type = each_div.find('ul', class_='b-icons m-separator')
                            if ul_car_type:
                                li_car_type = ul_car_type.find('li', class_='e-summary-icon m-demo')
                                if li_car_type:
                                    car_type = li_car_type.text.strip()
    
                            # Find all 'ul' elements with class 'b-icons m-separator'
                            ul_elements = each_div.find_all('ul', class_='b-icons m-separator')
                            for ul in ul_elements:
                                li_elements = ul.find_all('li')

                                # Check if there are more than two 'li' elements in the 'ul'
                                if len(li_elements) > 2:
                                    mileage_element = li_elements[-2].get_text(strip=True)
                                    mileage=mileage_element
                                    transmission=li_elements[-1].get_text(strip=True)
                                else:
                                    mileage=None 
                                    transmission=li_elements[-1].get_text(strip=True)
        
                            dealer = each_div.find('span', class_='e-dealer__ijJoWpcBtgDSTAs2').text.strip()
                            location = each_div.find('span', class_='e-suburb__UJOR_1tLqFkXpFjd').text.strip()

                            parts = location.split(',')
                            suburb, region = parts
                            
                            #car_data = {'Title': title, 'Car_ID':Car_ID ,'ExpectedPaymentPerMonth':expected_payment,'price': price, 'Mileage':mileage,'Timestamp':current_datetime,'CarType':car_type,'Transmission':transmission,'Dealer':dealer,'Suburb':suburb, 'Region':region,'url':found_link}  

                            car_data = {'title': title, 'Car_ID':Car_ID ,'expected_payment':expected_payment,'price': price, 'mileage':mileage,'current_datetime':current_datetime,'car_type':car_type,'transmission':transmission,'dealer':dealer,'suburb':suburb, 'region':region,'url':found_link}  

                            def clean_column(column):
                                value = car_data[column]
                                if value is None:
                                    return None
                                cleaned_column = car_data[column].splitlines()  
                                cleaned_column = [re.sub(r'[^\d.,]+', '', str(x)) if x is not None else x for x in cleaned_column] 
                                return cleaned_column[0] if cleaned_column else None
                            
                            # Columns to clean
                            #columns_to_clean = ['price','Mileage' ,'ExpectedPaymentPerMonth']
                            columns_to_clean = ['price','mileage' ,'expected_payment']


                            for column in columns_to_clean:
                                car_data[column] = clean_column(column)
                            #print(car_data)
                            
                            thread_data.append(car_data)                             
                                                                                                    
                                            
                    except Exception as e:
                        continue


async def main():
    async with aiohttp.ClientSession() as session:
        for page in range(start_page, last_page + 1):
            url = f"https://www.autotrader.co.za/cars-for-sale?pagenumber={str(page)}&sortorder=Newest&priceoption=RetailPrice"
            
            tasks.append(scrape_page(session, url, page))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    print('running')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    data_frame = pd.DataFrame()
    for car_data in thread_data:                
        data_series = pd.Series(car_data)  # Convert the dictionary to a Series
        data_frame = pd.concat([data_frame, data_series.to_frame().T], ignore_index=True)

              
        
    if last_page >= limit + 100:
        reset_page()
        reset_run()
        reset_run_retry()
        quit()
    else:
        if len(data_frame) == 2000:
            car_data_csv = data_frame.to_csv(encoding = "utf-8", index= False)
            sas_url= f"https://autotraderstorage.blob.core.windows.net/cardata/car_data_{run_number()}.csv?sv=2023-01-03&st=2024-11-09T12%3A34%3A15Z&se=2025-06-26T12%3A34%3A00Z&sr=c&sp=racwdl&sig=1h4aVM%2FuNibKPF8CmO3ki1EzCOjha0BQdp4j3F6rJBQ%3D"
            client = BlobClient.from_blob_url(sas_url)
            client.upload_blob(car_data_csv, overwrite=True)

            new_page_number()
            new_run_number()
            reset_run_retry()

        elif len(data_frame) > 0 and int(limit) in range(int(start_page), int(last_page+1)):
            car_data_csv = data_frame.to_csv(encoding = "utf-8", index= False)
            sas_url= f"https://autotraderstorage.blob.core.windows.net/cardata/car_data_{run_number()}.csv?sv=2023-01-03&st=2024-11-09T12%3A34%3A15Z&se=2025-06-26T12%3A34%3A00Z&sr=c&sp=racwdl&sig=1h4aVM%2FuNibKPF8CmO3ki1EzCOjha0BQdp4j3F6rJBQ%3D"
            client = BlobClient.from_blob_url(sas_url)
            client.upload_blob(car_data_csv, overwrite=True)

            new_page_number()
            new_run_number()
            reset_run_retry()

        elif run_number_retry() == 4:
            reset_page()
            reset_run()
            reset_run_retry()
            quit()

        else:
            new_run_number_retry()

       # token = 'glptt-9ad341ed3a51b05b3bbe2b78eaeb6300b005ac94'
       # response = requests.post(f'https://gitlab.com/api/v4/projects/50625156/ref/main/trigger/pipeline?token={token}')
