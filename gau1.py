import csv
import requests
from bs4 import BeautifulSoup
import random
import re
import time
from datetime import datetime
from requests.exceptions import ConnectTimeout
import math
#from azure.storage.blob import BlobClient
import datetime
import math
import pandas as pd



# URL of the Autotrader website
base_url = "https://www.autotrader.co.za"
start_page=500
#start_page=42

# Get the last page
#def get_last_page():
 #   response = requests.get("https://www.autotrader.co.za/cars-for-sale?sortorder=PriceLow&priceoption=RetailPrice")
  #  home_page = BeautifulSoup(response.content, 'html.parser')#
   # total_listings_element = home_page.find('span', class_='e-results-total')
    #total_listings = int(total_listings_element.text.replace(' ', ''))
    #listings_per_page = 20
    #last_page = math.ceil(total_listings / listings_per_page)
    #return last_page

# Call the function to get the last page
last_page = 501
#print(last_page)



pd_car_data = pd.DataFrame()

#ip_index = 0 
#while start_page <= last_page + 1:
for page in range(start_page, last_page + 1):

    # Check if 8 pages have been scraped
    if page % 2 == 0:
        # Generate a random sleep duration between 120 and 200 seconds
        sleep_duration = random.randint(30, 35)

        # Sleep for the random duration
        time.sleep(sleep_duration)      

    try:    
        # Use the current IP address for the request
        #proxies = {"http": 'http://'+ current_ip, "https": 'http://'+current_ip}
        response = requests.get(f"https://www.autotrader.co.za/cars-for-sale?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")#, proxies=proxies)
        if response.status_code==200:
            print(page)
        home_page = BeautifulSoup(response.content, 'lxml')            


        # Find all the car listings on the page 
        import re
        cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})
        for each_div in cars_containers:         

            # Find the link to the car listing
            for link in each_div.find_all('a', href=True):
                try:
                    if '?' not in link['href']:
                        pass
                    else:
                        found_link = (base_url + link['href'])  
                        #Car_ID = re.search(r'/(\d+)\?', found_link).group(1)                  
                except:
                    continue

                try:
                    res = requests.get(found_link)
                    html_content = res.content.decode('utf-8')  # Decode the content to a string

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
                    cleaned_html = emoji_pattern.sub(r'', html_content)                
                    car_soup = BeautifulSoup(cleaned_html, 'lxml')

                    car_id_match = re.search(r'/(\d+)\?', found_link)
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
                    current_datetime = datetime.datetime.now().strftime('%Y-%m-%d')

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

                    
                    for column in columns_to_clean:
                        car_data[column] = clean_column(column)
                    print(car_data)

                    pd_car_data_1 = pd.DataFrame([car_data])

                    if len(pd_car_data) == 0:
                        pd_car_data = pd.DataFrame([car_data])
                    else:
                        pd_car_data = pd.concat([pd_car_data, pd_car_data_1], axis=False)                

                except ConnectTimeout:
                    print(f"Connection timed out for link: {found_link}")
                    continue

        start_page += 1
    except Exception as e:
        print(e) 
        continue 
car_data_csv = pd_car_data.to_csv(index_label="idx", encoding = "utf-8")
pd_car_data.to_csv("all.csv",index=False, encoding = "utf-8")

#sas_url= f"https://stautotrader.blob.core.windows.net/cardata/autotrader_stage.csv?sv=2021-10-04&st=2023-09-29T10%3A42%3A52Z&se=2030-09-30T10%3A42%3A00Z&sr=c&sp=racwdxltf&sig=oGdEF5ixg%2BDOzuU1oO8105pJCGC7d0a26t2usZ1%2BybQ%3D"

#client = BlobClient.from_blob_url(sas_url)

#client.upload_blob(car_data_csv, overwrite=True)

