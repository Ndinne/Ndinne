from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import pandas as pd
import re
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings("ignore")

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument('--headless')  
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument("--log-level=3")  # Suppress logs
chrome_options.add_argument("--silent")
chrome_options.add_argument("--disable-logging")
chrome_options.add_argument("--disable-extensions")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)


import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobClient

def get_urls():
    sas_url = f"https://stautotrader.blob.core.windows.net/properties/Listingids.csv?sv=2021-10-04&ss=btqf&srt=sco&st=2023-10-17T07%3A39%3A17Z&se=2030-10-18T07%3A39%3A00Z&sp=rwdxftlacup&sig=%2BTFZttmuMZLkl%2Bq%2Bf2t%2FPNBSJkWUzw52PPp1sL9X8Wk%3D"
    client = BlobClient.from_blob_url(sas_url)
    blob = client.download_blob()

    blob_content = blob.readall()
    data = BytesIO(blob_content)
    data1 = pd.read_csv(data)

    # Select the first column for the specified range of rows
    selected_rows = data1.iloc[:, 0]
    return selected_rows


def ingest_data(all_data):
    start_time = time.time()
    if all_data:
        data_frame = pd.DataFrame(all_data)
        # Save DataFrame to CSV
        car_data_csv = data_frame.to_csv(encoding="utf-8", index=False)
        sas_url = f"https://stautotrader.blob.core.windows.net/properties/AirbnbListings{start_time}.csv?sv=2021-10-04&ss=btqf&srt=sco&st=2023-10-17T07%3A39%3A17Z&se=2030-10-18T07%3A39%3A00Z&sp=rwdxftlacup&sig=%2BTFZttmuMZLkl%2Bq%2Bf2t%2FPNBSJkWUzw52PPp1sL9X8Wk%3D"
        client = BlobClient.from_blob_url(sas_url)
        client.upload_blob(car_data_csv, overwrite=True) 

def generate_check_dates():
    current_date = datetime.now()
    start_year = current_date.year
    start_month = current_date.month + 1
    start_day = current_date.day

    check_in_dates = [
        datetime(start_year, start_month, start_day),
        datetime(start_year + (start_month + 3 - 1) // 12, (start_month + 3 - 1) % 12 + 1, 1),
        datetime(start_year + (start_month + 6 - 1) // 12, (start_month + 6 - 1) % 12 + 1, 1),
        datetime(start_year + (start_month + 9 - 1) // 12, (start_month + 9 - 1) % 12 + 1, 1),
        datetime(start_year + (start_month + 9 - 1) // 12, (start_month + 9 - 1) % 12 + 1, 1)
    ]

    check_out_dates = [date + timedelta(days=2) for date in check_in_dates]
    
    return check_in_dates, check_out_dates

def convert_date_string(date_str):
    if isinstance(date_str, datetime):
        return date_str
    return datetime.strptime(date_str, '%m/%d/%Y')


def clean_text(text):
    text = re.sub(r'[\r\n\t]', ' ', text)
    text = re.sub(r'Â·', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text

def extract_listing_data(soup, id):
    data = {}

    listingid = id
    data['listingid'] = listingid

    h1_tag = soup.find('h1')
    if h1_tag:
        data['title'] = clean_text(h1_tag.text)

    h2_tag = soup.find('h2')
    if h2_tag:
        data['locality'] = clean_text(h2_tag.text)

    desc_list = soup.find('ol').find_all('li')
    listing_features = " ".join(clean_text(i.text) for i in desc_list)
    data['listing_features'] = listing_features

    host_tag = soup.find('div', class_='t1pxe1a4')
    if host_tag:
        data['hostname'] = clean_text(host_tag.text)

    host_details = soup.find('div', class_='s1l7gi0l')
    if host_details:
        data['years_hosting'] = clean_text(host_details.text)

    try:
        location_tag = soup.find('div', class_='_1t2xqmi').find('h3', class_='hpipapi')
        if location_tag:
            data['location'] = clean_text(location_tag.text)
        else:
            data['location'] = None
    except AttributeError:
        data['location'] = None

    try:
        price_divs = soup.find_all('div', class_='_tr4owt')
        for div in price_divs:
            if 'x' in div.text.strip():
                price_per_night_tag = div.find('div', class_='l1x1206l')
                if price_per_night_tag:
                    price_per_night = clean_text(price_per_night_tag.text)
                    price_per_night = price_per_night.split(' ')[0].replace('Â', '').replace('ZAR', '')
                    data['price_per_night'] = price_per_night
            elif 'Cleaning' in div.text.strip():
                cleaning_fee_tag = div.find('span', class_='_1k4xcdh')
                if cleaning_fee_tag:
                    cleaning_fee = clean_text(cleaning_fee_tag.text)
                    cleaning_fee = cleaning_fee.replace('Â', '').replace('ZAR', '')
                    data['cleaning_fee'] = cleaning_fee
            elif 'service' in div.text.strip():
                service_fee_tag = div.find('span', class_='_1k4xcdh')
                if service_fee_tag:
                    service_fee = clean_text(service_fee_tag.text)
                    service_fee = service_fee.replace('Â', '').replace('ZAR', '')
                    data['service_fee'] = service_fee
    except IndexError as e:
        print(f"Error accessing row data: {e}")

    unblocked_days = []
    blocked_days_dict = {}
    
    div_tags = soup.find_all('div', {'data-is-day-blocked': True})

    for div in div_tags:
        full_date = div.get('data-testid').replace('calendar-day-', '')
        is_blocked = div['data-is-day-blocked'] == 'true'

        if is_blocked:
            blocked_days_dict[full_date] = 'blocked'
        else:
            unblocked_days.append(full_date) 
            unblocked_days_final.extend(unblocked_days)  
    
    blocked_days = str(blocked_days_dict)
    data['blocked_days'] = blocked_days
    
    Timestamp = datetime.now().strftime('%Y-%m-%d')
    data["Timestamp"] = Timestamp

    return data

all_listing_ids_final = get_urls()

all_data = []
blocked_days = []
unblocked_days_final = []

def scrape_airbnb(id, check_in, check_out):
    check_in_str = check_in.strftime('%Y-%m-%d')
    check_out_str = check_out.strftime('%Y-%m-%d')
    url = f"https://www.airbnb.co.za/rooms/{id}?adults=1&category_tag=Tag%3A8678&enable_m3_private_room=true&photo_id=1602362717&search_mode=regular_search&source_impression_id=p3_1721650674_P36bK5Ps7ufayXNj&previous_page_section_name=1000&federated_search_id=9cc4e6a6-d201-4efc-b0a6-142e51c608da&guests=1&check_in={check_in_str}&check_out={check_out_str}"
    driver.get(url)

    wait = WebDriverWait(driver, 40)
    main_body_xpath = '//*[@id="react-application"]'
    main_body_element = wait.until(EC.presence_of_element_located((By.XPATH, main_body_xpath)))
    time.sleep(6)

    main_body_html = main_body_element.get_attribute('outerHTML')
    soup = BeautifulSoup(main_body_html, 'html.parser')

    listing_data = extract_listing_data(soup, id)
    all_data.append(listing_data)

try:
    request_count = 0
    check_in_run_count = 0

    for id in all_listing_ids_final:
        check_in_dates, check_out_dates = generate_check_dates()
        for idx, (check_in, check_out) in enumerate(zip(check_in_dates, check_out_dates)):
            print(id)
            scrape_airbnb(id, check_in, check_out)
            if idx == 4:
                unblocked_days_final =[convert_date_string(date) for date in unblocked_days_final]
                if unblocked_days_final:
                    check_in4 = unblocked_days_final.pop(0)
                    check_in = check_in4
                    check_out = check_in4 + timedelta(days=1)

            request_count += 1
            check_in_run_count += 1
            if request_count % 20 == 0 or request_count == len(all_listing_ids_final) :
                ingest_data(all_data)
                print("data ingested...")
                time.sleep(70) 
                all_data = []

finally:
    driver.quit()
