import aiohttp
import asyncio
from bs4 import BeautifulSoup
import re
from azure.storage.blob import BlobClient
import requests
from bs4 import BeautifulSoup
import time
import datetime
from datetime import datetime
from requests.exceptions import ConnectTimeout
import math
import pandas as pd
import re

found_links = [
        'https://www.autotrader.co.za/car-for-sale/mahindra/pik-up/2.2/26904505?',
        'https://www.autotrader.co.za/car-for-sale/mahindra/pik-up/2.2/27008449?',
        'https://www.autotrader.co.za/car-for-sale/mahindra/pik-up/2.2/27209192?',
        'https://www.autotrader.co.za/car-for-sale/mahindra/pik-up/2.2/27210875?',
        'https://www.autotrader.co.za/car-for-sale/mahindra/pik-up/2.2/27209191?'
    ]

pd_car_data = pd.DataFrame()

base_url = "https://www.autotrader.co.za"

# Define the scraping function to scrape car data
async def scrape_car_data(session, url):
    global pd_car_data
    car_data = {}
    try:
        async with session.get(url) as response:
            if response.status == 200 :
                page_content = await response.text()
                import re

                # Define a regex pattern to match emojis
                emoji_pattern = re.compile(
                    "["
                    "\U0001F600-\U0001F64F"  # Emoticons
                    "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
                    "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
                    "\U0001F700-\U0001F77F"  # Alchemical Symbols
                    "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                    "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                    "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                    "\U0001FA00-\U0001FA6F"  # Chess Symbols
                    "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                    "\U00002702-\U000027B0"  # Dingbats
                    "\U000024C2-\U0001F251" 
                    "]+",
                    flags=re.UNICODE,
                )

                # Remove emojis using the regex pattern
                cleaned_html = emoji_pattern.sub(r'', page_content)                
                car_soup = BeautifulSoup(cleaned_html, 'lxml')
                
                car_id_match = re.search(r'/(\d+)\?', url)
                if car_id_match:
                    Car_ID = car_id_match.group(1)
                else:
                    Car_ID = None
                title = car_soup.find(class_='e-listing-title').text.strip()
                ul = car_soup.find('ul', class_='b-breadcrumbs')
                li_items = ul.find_all('li')

                #car_id = li_items[6].find('span', itemprop='name').text.strip()
                location = li_items[2].text.strip()
                brand = li_items[3].text.strip()
                model = li_items[4].text.strip()
                variant = li_items[5].text.strip()

                ListingSuburbName = re.search(r'ga\(.+?\'dimension102\',\s*\'(.+?)\'\);', cleaned_html)  
                suburb= ListingSuburbName.group(1)                       

                price = car_soup.find('div', class_='e-price').text.strip() if car_soup.find('div', class_='e-price') else None
                expected_payment = car_soup.find('a', class_='e-calculator-link').text.strip() if car_soup.find('a', class_='e-calculator-link') else None
                # Check if car type is listed under a specific list item
                li_car_type = car_soup.find('li', class_='e-summary-icon m-type')
                if li_car_type:
                    car_type = li_car_type.text.strip()

                # Check if car type is listed within the specified unordered list
                ul_car_type = car_soup.find('ul', class_='b-icons m-large m-icon')
                if ul_car_type:
                    li_car_type = ul_car_type.find('li', class_='e-summary-icon m-demo')
                    if li_car_type:
                        car_type = li_car_type.text.strip()
                #car_type = car_soup.find('li', class_='e-summary-icon m-type').text.strip() if car_soup.find('li', class_='e-summary-icon m-type') else None
                registration_year = car_soup.find('li', title='Registration Year').text.strip() if car_soup.find('li', title='Registration Year') else None
                mileage = car_soup.find('li', title='Mileage').text.strip().split(' ')[0] if car_soup.find('li', title='Mileage') else None
                transmission = car_soup.find('li', title='Transmission').text.strip() if car_soup.find('li', title='Transmission') else None
                fuel_type = car_soup.find('li', title='Fuel Type').text.strip() if car_soup.find('li', title='Fuel Type') else None
                price_rating = car_soup.find('span', class_='b-price-rating').text.strip() if car_soup.find('span', class_='b-price-rating') else None
                
                dealer = car_soup.find('a', class_='e-dealer-link').text.strip() if car_soup.find('a', class_='e-dealer-link') else None
                dealer_link = car_soup.find('a', class_='e-dealer-link')
                # Extract the link (href) attribute
                basedealer="https://www.autotrader.co.za"
                if dealer_link:
                    link_dealer = dealer_link
                    DealerUrl=(basedealer + link_dealer['href'])
                    
                else:
                    DealerUrl = None

                last_updated = car_soup.find(string='Last Updated').find_next('div').text.strip() if car_soup.find(string='Last Updated') else None
                previous_owners = car_soup.find(string='Previous Owners').find_next('div').text.strip() if car_soup.find(string='Previous Owners') else None
                manufacturers_colour = car_soup.find(string='Colour').find_next('div').text.strip() if car_soup.find(string='Colour') else None
                if manufacturers_colour == None:
                    manufacturers_colour = car_soup.find(string='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(string='Manufacturers Colour') else None
                body_type = car_soup.find(string='Body Type').find_next('div').text.strip() if car_soup.find(string='Body Type') else None
                warranty_remaining = car_soup.find(string='Warranty Remaining').find_next('div').text.strip() if car_soup.find(string='Warranty Remaining') else None
                service_history = car_soup.find(string='Service History').find_next('div').text.strip() if car_soup.find(string='Service History') else None

                introduction_date = car_soup.find(string='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Introduction date') else None
                end_date = car_soup.find(string='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(string='End date') else None
                service_interval_distance = car_soup.find(string='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Service interval distance') else None

                engine_position = car_soup.find(string='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Engine position') else None
                engine_detail = car_soup.find(string='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Engine detail') else None
                engine_capacity = car_soup.find(string='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Engine capacity (litre)') else None
                cylinder_layout = car_soup.find(string='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Cylinder layout and quantity') else None
                fuel_type_engine = car_soup.find(string='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Fuel type') else None
                fuel_capacity = car_soup.find(string='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Fuel capacity') else None
                fuel_consumption = car_soup.find(string='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Fuel consumption (average)') else None
                fuel_range = car_soup.find(string='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Fuel range (average)') else None
                power_max = car_soup.find(string='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Power maximum (detail)') else None
                torque_max = car_soup.find(string='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Torque maximum') else None
                acceleration = car_soup.find(string='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Acceleration 0-100 km/h') else None
                maximum_speed = car_soup.find(string='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(string='Maximum/top speed') else None
                co2_emissions = car_soup.find(string='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(string='CO2 emissions (average)') else None
                #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                current_datetime = datetime.now().strftime('%Y-%m-%d')

                ListingProvinceName = re.search(r'ga\(.+?\'dimension98\',\s*\'(.+?)\'\);', cleaned_html)
                province_name= ListingProvinceName.group(1)

                car_data = {
                    'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                    'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                    'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                    'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                    'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                    'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                    'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                    'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                    'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name,'Car_ID':Car_ID,'DealerUrl':DealerUrl
                }                                         


                import re

                def clean_column(column):
                    value = car_data[column]
                    if value is None:
                        return None
                    cleaned_column = car_data[column].splitlines()
                    cleaned_column = [x.split('total')[-1].strip() if 'total' in str(x) else x for x in cleaned_column]
                    cleaned_column = [x.split('max')[-1].strip() if 'max' in str(x) else x for x in cleaned_column]
                    cleaned_column = [x.split('/')[0].strip() if '/' in str(x) else x for x in cleaned_column]
                    cleaned_column = [x.split('-')[0].strip() if '-' in str(x) else x for x in cleaned_column]
                    cleaned_column = [x.split('(opt')[0].strip() if '(opt' in str(x) else x for x in cleaned_column]
                    cleaned_column = [re.sub(r'^.*?\((\d+)\).*$', r'\1', str(x)) if x is not None else x for x in cleaned_column]
                    cleaned_column = [re.sub(r'\(.*', '', str(x)) if x is not None else x for x in cleaned_column]
                    cleaned_column = [re.findall(r'^(\d+)-', str(x))[0] if re.findall(r'^\d+-', str(x)) else x for x in cleaned_column]
                    cleaned_column = [re.sub(r'[^\d.,]+', '', str(x)) if x is not None else x for x in cleaned_column]
                    cleaned_column = [x.replace('.0', '') if x is not None else x for x in cleaned_column]
                    cleaned_column = [x.replace(',', '.') if x is not None else x for x in cleaned_column]
                    cleaned_column = [x.strip() if x is not None else x for x in cleaned_column]
                    cleaned_column = [x[0] if isinstance(x, list) else x for x in cleaned_column]
                    return cleaned_column[0] if cleaned_column else None
                # Columns to clean
                columns_to_clean = ['price','mileage' ,'expected_payment', 'service_interval_distance',
                                    'engine_capacity', 'fuel_capacity', 'fuel_consumption', 'fuel_range',
                                    'power_max', 'torque_max', 'acceleration','maximum_speed',
                                    'co2_emissions']

                # Clean the specified columns in the car_data dictionary
                for column in columns_to_clean:
                    car_data[column] = clean_column(column)

                pd_car_data_1 = pd.DataFrame([car_data])

                if len(pd_car_data) == 0:
                    pd_car_data = pd.DataFrame([car_data])
                else:
                    pd_car_data = pd.concat([pd_car_data, pd_car_data_1], axis=False)
                    
                #print(pd_car_data)


    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")

async def main():

    # Now that you have collected the found links, create a new session for scraping car data
    async with aiohttp.ClientSession() as session_data:
        tasks2 = [scrape_car_data(session_data, url) for url in found_links]
        await asyncio.gather(*tasks2)
    

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    print(pd_car_data)
    print(f"Total cars found: {len(found_links)}")
