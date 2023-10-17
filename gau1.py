import threading

def code_1():
    #!/usr/bin/env python
    # coding: utf-8

    import requests
    from bs4 import BeautifulSoup
    import random
    import pyodbc
    import re
    import time
    import datetime
    from datetime import datetime
    from requests.exceptions import ConnectTimeout
    import math
    # URL of the Autotrader website
    base_url = "https://www.autotrader.co.za"

    # define the connection details
    server = 'web-development.database.windows.net'
    database = 'graduates'
    username = 'Canvas'
    password = 'Dut950505'
    driver = '{ODBC Driver 17 for SQL Server}'

    # create a connection
    conn = pyodbc.connect(
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'DRIVER={driver};')

    # create a cursor
    cursor = conn.cursor()

    # Step : Define the table name
    table_name = 'l_All'
    # Check if the table exists, if not create it
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ( Car_ID NVARCHAR(50),Title NVARCHAR(MAX), Region NVARCHAR(100), Make NVARCHAR(100), Model NVARCHAR(100), Variant NVARCHAR(100), Suburb NVARCHAR(100), Province NVARCHAR (50),Price NVARCHAR(100), ExpectedPaymentPerMonth NVARCHAR(100), CarType NVARCHAR(100), RegistrationYear NVARCHAR(100), Mileage NVARCHAR(100), Transmission NVARCHAR(100), FuelType NVARCHAR(100), PriceRating NVARCHAR(100), Dealer NVARCHAR(100), LastUpdated NVARCHAR(100), PreviousOwners NVARCHAR(100), ManufacturersColour NVARCHAR(100), BodyType NVARCHAR(100),ServiceHistory NVARCHAR(100), WarrantyRemaining NVARCHAR(100), IntroductionDate NVARCHAR(100), EndDate NVARCHAR(100), ServiceIntervalDistance NVARCHAR(100), EnginePosition NVARCHAR(100), EngineDetail NVARCHAR(100), EngineCapacity NVARCHAR(100), CylinderLayoutAndQuantity NVARCHAR(100), FuelTypeEngine NVARCHAR(100), FuelCapacity NVARCHAR(100), FuelConsumption NVARCHAR(100), FuelRange NVARCHAR(100), PowerMaximum NVARCHAR(100), TorqueMaximum NVARCHAR(100), Acceleration NVARCHAR(100), MaximumSpeed NVARCHAR(100), CO2Emissions NVARCHAR(100), Version INT,DealerUrl NVARCHAR(100), Timestamp DATETIME)")
    # Commit the changes and close the connection
    conn.commit()


    # Function to get the last scraped page and province
    def get_last_scraped_page_and_year():
        cursor = conn.cursor()
        select_query = "SELECT page, province FROM autotpage WHERE id =19" 
        cursor.execute(select_query)
        record = cursor.fetchone()
        if record is not None:
            return record.page, record.province
        else:
            return 1, 1  # Default values if the record doesn't exist

    def update_last_scraped_page_and_year(page,province):
        cursor = conn.cursor()
        update_query = "UPDATE autotpage SET page = ?, province = ? WHERE id =19"
        values = (page, province)
        cursor.execute(update_query, values)
        conn.commit()

    # Set the desired execution time to one hour (3600 seconds)
    execution_time = time.time() + 3000

    start_page, start_province  = get_last_scraped_page_and_year()

    execution_time = time.time() + 3000
    page=start_page
    province=start_province
    #last_page = get_last_page(province)

    provinces_to_scrape = [1,1,1,1,1,1,1,1,1,1,1,1]

    for province in provinces_to_scrape: 
        last_page = 220
        for page in range(start_page, last_page + 1):
            response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-{province}?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")

            home_page = BeautifulSoup(response.content, 'lxml')



            # Find all the car listings on the page 
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})



            for each_div in cars_containers:
                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    if time.time() >= execution_time :
                        execution_time += 120
                        time.sleep(60)
                    try:
                        found_link = (base_url + link['href'])
                        Car_ID = re.search(r'/(\d+)\?', found_link).group(1)
                    except:
                         continue

                    try:
                        # Get the HTML content of the car listing page
                        res = requests.get(found_link, timeout=10)
                                            # Assuming 'res.content' contains your HTML content

                        # Assuming 'res.content' contains your HTML content
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

                        # Parse the cleaned HTML content
                        car_soup = BeautifulSoup(cleaned_html, 'lxml')
                        # Step : Extract data from the car listing page
                        title = car_soup.find(class_='e-listing-title').text.strip()



                        ul = car_soup.find('ul', class_='b-breadcrumbs')
                        li_items = ul.find_all('li')



                        #car_id = li_items[6].find('span', itemprop='name').text.strip()
                        location = li_items[2].text.strip()
                        brand = li_items[3].text.strip()
                        model = li_items[4].text.strip()
                        variant = li_items[5].text.strip()

                        # Find the <div class="b-rate-container"> element
                        rating_container = car_soup.find('div', class_='b-rate-container')
                        # Find the <div class="col-8"> element within the rating_container
                        suburb_div = rating_container.find_next('div', class_='col-8')
                        # Find the <span> element within the suburb_div
                        suburb_element = suburb_div.find('span')
                        if suburb_element:
                            suburb = suburb_element.get_text(strip=True)
                        else:
                            suburb = None                        



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
                            car_data['DealerUrl']=DealerUrl



                        last_updated = car_soup.find(text='Last Updated').find_next('div').text.strip() if car_soup.find(text='Last Updated') else None
                        previous_owners = car_soup.find(text='Previous Owners').find_next('div').text.strip() if car_soup.find(text='Previous Owners') else None
                        manufacturers_colour = car_soup.find(text='Colour').find_next('div').text.strip() if car_soup.find(text='Colour') else None
                        if manufacturers_colour == None:
                            manufacturers_colour = car_soup.find(text='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(text='Manufacturers Colour') else None
                        body_type = car_soup.find(text='Body Type').find_next('div').text.strip() if car_soup.find(text='Body Type') else None
                        warranty_remaining = car_soup.find(text='Warranty Remaining').find_next('div').text.strip() if car_soup.find(text='Warranty Remaining') else None
                        service_history = car_soup.find(text='Service History').find_next('div').text.strip() if car_soup.find(text='Service History') else None




                        introduction_date = car_soup.find(text='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Introduction date') else None
                        end_date = car_soup.find(text='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='End date') else None
                        service_interval_distance = car_soup.find(text='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Service interval distance') else None




                        engine_position = car_soup.find(text='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine position') else None
                        engine_detail = car_soup.find(text='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine detail') else None
                        engine_capacity = car_soup.find(text='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine capacity (litre)') else None
                        cylinder_layout = car_soup.find(text='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Cylinder layout and quantity') else None
                        fuel_type_engine = car_soup.find(text='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel type') else None
                        fuel_capacity = car_soup.find(text='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel capacity') else None
                        fuel_consumption = car_soup.find(text='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel consumption (average)') else None
                        fuel_range = car_soup.find(text='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel range (average)') else None
                        power_max = car_soup.find(text='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Power maximum (detail)') else None
                        torque_max = car_soup.find(text='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Torque maximum') else None
                        acceleration = car_soup.find(text='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Acceleration 0-100 km/h') else None
                        maximum_speed = car_soup.find(text='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Maximum/top speed') else None
                        co2_emissions = car_soup.find(text='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='CO2 emissions (average)') else None
                        #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        current_datetime = datetime.now().strftime('%Y-%m-%d')



                        province_names = {
                            1: 'Gauteng',
                            2: 'Kwazulu Natal',
                            3: 'Free State',
                            5: 'Mpumalanga',
                            6: 'North West',
                            7: 'Eastern Cape',
                            8: 'Northern Cape',
                            9: 'Western Cape',
                            14: 'Limpopo'
                        }
                        province_name = province_names.get(province)



                        car_data = {
                            'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                            'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                            'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                            'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                            'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                            'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                            'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                            'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                            'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name
                        }                     



                    except ConnectTimeout:
                        print(f"Connection timed out for link: {found_link}")
                        continue



                    car_data['Car_ID'] = Car_ID
                    car_data['DealerUrl']=DealerUrl
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
                        cleaned_column = [x.replace('.', ',') if x is not None else x for x in cleaned_column]
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


                    cursor.execute(
                        f"INSERT INTO {table_name} (Title, Car_ID, Region, Make, Model, Variant, Suburb,Province, Price, ExpectedPaymentPerMonth, CarType, RegistrationYear, Mileage, Transmission, FuelType, PriceRating, Dealer, LastUpdated, PreviousOwners, ManufacturersColour, BodyType, ServiceHistory, WarrantyRemaining, IntroductionDate, EndDate, ServiceIntervalDistance, EnginePosition, EngineDetail, EngineCapacity, CylinderLayoutAndQuantity, FuelTypeEngine, FuelCapacity, FuelConsumption, FuelRange, PowerMaximum, TorqueMaximum, Acceleration, MaximumSpeed, CO2Emissions, Version, DealerUrl, Timestamp) VALUES ( ?,?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        car_data['title'], car_data['Car_ID'], car_data['location'], car_data['brand'], car_data['model'], car_data['variant'], car_data['suburb'], car_data['province_name'],car_data['price'], car_data['expected_payment'], car_data['car_type'], car_data['registration_year'],
                        car_data['mileage'], car_data['transmission'], car_data['fuel_type'], car_data['price_rating'], car_data['dealer'], car_data['last_updated'], car_data['previous_owners'], car_data['manufacturers_colour'],
                        car_data['body_type'], car_data['service_history'], car_data['warranty_remaining'], car_data['introduction_date'], car_data['end_date'], car_data['service_interval_distance'], car_data['engine_position'],
                        car_data['engine_detail'], car_data['engine_capacity'], car_data['cylinder_layout'], car_data['fuel_type_engine'], car_data['fuel_capacity'], car_data['fuel_consumption'],
                        car_data['fuel_range'], car_data['power_max'], car_data['torque_max'], car_data['acceleration'], car_data['maximum_speed'], car_data['co2_emissions'], car_data['DealerUrl'], car_data['current_datetime'])

                    # Commit the changes
                    conn.commit()
                    update_last_scraped_page_and_year(page,province)   
        

        start_page = 1
        page=1
        update_last_scraped_page_and_year(page,province)    

    conn.close()     

def code_2():
    # Your code for task 2
    #!/usr/bin/env python
    # coding: utf-8

    import requests
    from bs4 import BeautifulSoup
    import random
    import pyodbc
    import re
    import time
    import datetime
    from datetime import datetime
    from requests.exceptions import ConnectTimeout
    import math
    # URL of the Autotrader website
    base_url = "https://www.autotrader.co.za"

    # define the connection details
    server = 'web-development.database.windows.net'
    database = 'graduates'
    username = 'Canvas'
    password = 'Dut950505'
    driver = '{ODBC Driver 17 for SQL Server}'

    # create a connection
    conn = pyodbc.connect(
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'DRIVER={driver};')

    # create a cursor
    cursor = conn.cursor()

    # Step : Define the table name
    table_name = 'l_All'
    # Check if the table exists, if not create it
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ( Car_ID NVARCHAR(50),Title NVARCHAR(MAX), Region NVARCHAR(100), Make NVARCHAR(100), Model NVARCHAR(100), Variant NVARCHAR(100), Suburb NVARCHAR(100), Province NVARCHAR (50),Price NVARCHAR(100), ExpectedPaymentPerMonth NVARCHAR(100), CarType NVARCHAR(100), RegistrationYear NVARCHAR(100), Mileage NVARCHAR(100), Transmission NVARCHAR(100), FuelType NVARCHAR(100), PriceRating NVARCHAR(100), Dealer NVARCHAR(100), LastUpdated NVARCHAR(100), PreviousOwners NVARCHAR(100), ManufacturersColour NVARCHAR(100), BodyType NVARCHAR(100),ServiceHistory NVARCHAR(100), WarrantyRemaining NVARCHAR(100), IntroductionDate NVARCHAR(100), EndDate NVARCHAR(100), ServiceIntervalDistance NVARCHAR(100), EnginePosition NVARCHAR(100), EngineDetail NVARCHAR(100), EngineCapacity NVARCHAR(100), CylinderLayoutAndQuantity NVARCHAR(100), FuelTypeEngine NVARCHAR(100), FuelCapacity NVARCHAR(100), FuelConsumption NVARCHAR(100), FuelRange NVARCHAR(100), PowerMaximum NVARCHAR(100), TorqueMaximum NVARCHAR(100), Acceleration NVARCHAR(100), MaximumSpeed NVARCHAR(100), CO2Emissions NVARCHAR(100), Version INT,DealerUrl NVARCHAR(100), Timestamp DATETIME)")
    # Commit the changes and close the connection
    conn.commit()


    def get_last_scraped_page_and_year():
        cursor = conn.cursor()
        select_query = "SELECT page, province FROM autotpage WHERE id =20" 
        cursor.execute(select_query)
        record = cursor.fetchone()
        if record is not None:
            return record.page, record.province
        else:
            return 220, 1  # Default values if the record doesn't exist

    def update_last_scraped_page_and_year(page,province):
        cursor = conn.cursor()
        update_query = "UPDATE autotpage SET page = ?, province = ? WHERE id =20"
        values = (page, province)
        cursor.execute(update_query, values)
        conn.commit()  

    # Function to get the last page for a specific year
    #def get_last_page(province):
        #response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/kwazulu-natal/p-{province}")

        #home_page = BeautifulSoup(response.content, 'lxml')

        #total_listings_element = home_page.find('span', class_='e-results-total')
        #total_listings = int(total_listings_element.text.replace(' ', ''))

        # Determine the number of pages based on the number of listings per page (e.g., 24 listings per page)
        #listings_per_page = 20
        #last_page = math.ceil(total_listings / listings_per_page)

        #return last_page


    # Set the desired execution time to one hour (3600 seconds)
    execution_time = time.time() + 3000

    start_page, start_province  = get_last_scraped_page_and_year()

    execution_time = time.time() + 3000
    page=start_page
    province=start_province
    #last_page = get_last_page(province)

    provinces_to_scrape = [1,1,1,1,1,1,1,1,1,1,1,1]

    for province in provinces_to_scrape: 
        last_page = 440
        for page in range(start_page, last_page + 1):
            response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-{province}?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")

            home_page = BeautifulSoup(response.content, 'lxml')


            # Find all the car listings on the page 
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})



            for each_div in cars_containers:
                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    if time.time() >= execution_time :
                        execution_time += 120
                        time.sleep(60)
                    try:
                        found_link = (base_url + link['href'])
                        Car_ID = re.search(r'/(\d+)\?', found_link).group(1)
                    except:
                         continue

                    try:
                        # Get the HTML content of the car listing page
                        res = requests.get(found_link, timeout=10)
                                            # Assuming 'res.content' contains your HTML content

                        # Assuming 'res.content' contains your HTML content
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

                        # Parse the cleaned HTML content
                        car_soup = BeautifulSoup(cleaned_html, 'lxml')
                        # Step : Extract data from the car listing page
                        title = car_soup.find(class_='e-listing-title').text.strip()



                        ul = car_soup.find('ul', class_='b-breadcrumbs')
                        li_items = ul.find_all('li')



                        #car_id = li_items[6].find('span', itemprop='name').text.strip()
                        location = li_items[2].text.strip()
                        brand = li_items[3].text.strip()
                        model = li_items[4].text.strip()
                        variant = li_items[5].text.strip()

                        # Find the <div class="b-rate-container"> element
                        rating_container = car_soup.find('div', class_='b-rate-container')
                        # Find the <div class="col-8"> element within the rating_container
                        suburb_div = rating_container.find_next('div', class_='col-8')
                        # Find the <span> element within the suburb_div
                        suburb_element = suburb_div.find('span')
                        if suburb_element:
                            suburb = suburb_element.get_text(strip=True)
                        else:
                            suburb = None                        



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



                        last_updated = car_soup.find(text='Last Updated').find_next('div').text.strip() if car_soup.find(text='Last Updated') else None
                        previous_owners = car_soup.find(text='Previous Owners').find_next('div').text.strip() if car_soup.find(text='Previous Owners') else None
                        manufacturers_colour = car_soup.find(text='Colour').find_next('div').text.strip() if car_soup.find(text='Colour') else None
                        if manufacturers_colour == None:
                            manufacturers_colour = car_soup.find(text='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(text='Manufacturers Colour') else None
                        body_type = car_soup.find(text='Body Type').find_next('div').text.strip() if car_soup.find(text='Body Type') else None
                        warranty_remaining = car_soup.find(text='Warranty Remaining').find_next('div').text.strip() if car_soup.find(text='Warranty Remaining') else None
                        service_history = car_soup.find(text='Service History').find_next('div').text.strip() if car_soup.find(text='Service History') else None




                        introduction_date = car_soup.find(text='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Introduction date') else None
                        end_date = car_soup.find(text='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='End date') else None
                        service_interval_distance = car_soup.find(text='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Service interval distance') else None




                        engine_position = car_soup.find(text='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine position') else None
                        engine_detail = car_soup.find(text='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine detail') else None
                        engine_capacity = car_soup.find(text='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine capacity (litre)') else None
                        cylinder_layout = car_soup.find(text='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Cylinder layout and quantity') else None
                        fuel_type_engine = car_soup.find(text='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel type') else None
                        fuel_capacity = car_soup.find(text='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel capacity') else None
                        fuel_consumption = car_soup.find(text='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel consumption (average)') else None
                        fuel_range = car_soup.find(text='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel range (average)') else None
                        power_max = car_soup.find(text='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Power maximum (detail)') else None
                        torque_max = car_soup.find(text='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Torque maximum') else None
                        acceleration = car_soup.find(text='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Acceleration 0-100 km/h') else None
                        maximum_speed = car_soup.find(text='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Maximum/top speed') else None
                        co2_emissions = car_soup.find(text='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='CO2 emissions (average)') else None
                        #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        current_datetime = datetime.now().strftime('%Y-%m-%d')



                        province_names = {
                            1: 'Gauteng',
                            2: 'Kwazulu Natal',
                            3: 'Free State',
                            5: 'Mpumalanga',
                            6: 'North West',
                            7: 'Eastern Cape',
                            8: 'Northern Cape',
                            9: 'Western Cape',
                            14: 'Limpopo'
                        }
                        province_name = province_names.get(province)



                        car_data = {
                            'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                            'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                            'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                            'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                            'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                            'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                            'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                            'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                            'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name
                        }                      



                    except ConnectTimeout:
                        print(f"Connection timed out for link: {found_link}")
                        continue



                    car_data['Car_ID'] = Car_ID
                    car_data['DealerUrl']=DealerUrl
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
                        cleaned_column = [x.replace('.', ',') if x is not None else x for x in cleaned_column]
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


                    cursor.execute(
                        f"INSERT INTO {table_name} (Title, Car_ID, Region, Make, Model, Variant, Suburb,Province, Price, ExpectedPaymentPerMonth, CarType, RegistrationYear, Mileage, Transmission, FuelType, PriceRating, Dealer, LastUpdated, PreviousOwners, ManufacturersColour, BodyType, ServiceHistory, WarrantyRemaining, IntroductionDate, EndDate, ServiceIntervalDistance, EnginePosition, EngineDetail, EngineCapacity, CylinderLayoutAndQuantity, FuelTypeEngine, FuelCapacity, FuelConsumption, FuelRange, PowerMaximum, TorqueMaximum, Acceleration, MaximumSpeed, CO2Emissions, Version, DealerUrl, Timestamp) VALUES ( ?,?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        car_data['title'], car_data['Car_ID'], car_data['location'], car_data['brand'], car_data['model'], car_data['variant'], car_data['suburb'], car_data['province_name'],car_data['price'], car_data['expected_payment'], car_data['car_type'], car_data['registration_year'],
                        car_data['mileage'], car_data['transmission'], car_data['fuel_type'], car_data['price_rating'], car_data['dealer'], car_data['last_updated'], car_data['previous_owners'], car_data['manufacturers_colour'],
                        car_data['body_type'], car_data['service_history'], car_data['warranty_remaining'], car_data['introduction_date'], car_data['end_date'], car_data['service_interval_distance'], car_data['engine_position'],
                        car_data['engine_detail'], car_data['engine_capacity'], car_data['cylinder_layout'], car_data['fuel_type_engine'], car_data['fuel_capacity'], car_data['fuel_consumption'],
                        car_data['fuel_range'], car_data['power_max'], car_data['torque_max'], car_data['acceleration'], car_data['maximum_speed'], car_data['co2_emissions'], car_data['DealerUrl'], car_data['current_datetime'])

                    # Commit the changes
                    conn.commit()
                    update_last_scraped_page_and_year(page,province)


        start_page = 220
        page=220
        update_last_scraped_page_and_year(page,province)    

    conn.close()  

def code_3():
    # Your code for task 3
    import requests
    from bs4 import BeautifulSoup
    import random
    import pyodbc
    import re
    import time
    import datetime
    from datetime import datetime
    from requests.exceptions import ConnectTimeout
    import math
    # URL of the Autotrader website
    base_url = "https://www.autotrader.co.za"

    # define the connection details
    server = 'web-development.database.windows.net'
    database = 'graduates'
    username = 'Canvas'
    password = 'Dut950505'
    driver = '{ODBC Driver 17 for SQL Server}'

    # create a connection
    conn = pyodbc.connect(
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'DRIVER={driver};')

    # create a cursor
    cursor = conn.cursor()

    # Step : Define the table name
    table_name = 'l_All'
    # Check if the table exists, if not create it
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ( Car_ID NVARCHAR(50),Title NVARCHAR(MAX), Region NVARCHAR(100), Make NVARCHAR(100), Model NVARCHAR(100), Variant NVARCHAR(100), Suburb NVARCHAR(100), Province NVARCHAR (50),Price NVARCHAR(100), ExpectedPaymentPerMonth NVARCHAR(100), CarType NVARCHAR(100), RegistrationYear NVARCHAR(100), Mileage NVARCHAR(100), Transmission NVARCHAR(100), FuelType NVARCHAR(100), PriceRating NVARCHAR(100), Dealer NVARCHAR(100), LastUpdated NVARCHAR(100), PreviousOwners NVARCHAR(100), ManufacturersColour NVARCHAR(100), BodyType NVARCHAR(100),ServiceHistory NVARCHAR(100), WarrantyRemaining NVARCHAR(100), IntroductionDate NVARCHAR(100), EndDate NVARCHAR(100), ServiceIntervalDistance NVARCHAR(100), EnginePosition NVARCHAR(100), EngineDetail NVARCHAR(100), EngineCapacity NVARCHAR(100), CylinderLayoutAndQuantity NVARCHAR(100), FuelTypeEngine NVARCHAR(100), FuelCapacity NVARCHAR(100), FuelConsumption NVARCHAR(100), FuelRange NVARCHAR(100), PowerMaximum NVARCHAR(100), TorqueMaximum NVARCHAR(100), Acceleration NVARCHAR(100), MaximumSpeed NVARCHAR(100), CO2Emissions NVARCHAR(100), Version INT,DealerUrl NVARCHAR(100), Timestamp DATETIME)")
    # Commit the changes and close the connection
    conn.commit()


    def get_last_scraped_page_and_year():
        cursor = conn.cursor()
        select_query = "SELECT page, province FROM autotpage WHERE id =21" 
        cursor.execute(select_query)
        record = cursor.fetchone()
        if record is not None:
            return record.page, record.province
        else:
            return 440, 1  # Default values if the record doesn't exist

    def update_last_scraped_page_and_year(page,province):
        cursor = conn.cursor()
        update_query = "UPDATE autotpage SET page = ?, province = ? WHERE id =21"
        values = (page, province)
        cursor.execute(update_query, values)
        conn.commit() 

    # Function to get the last page for a specific year
    #def get_last_page(province):
    response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/kwazulu-natal/p-1")

    home_page = BeautifulSoup(response.content, 'lxml')

    total_listings_element = home_page.find('span', class_='e-results-total')
    total_listings = int(total_listings_element.text.replace(' ', ''))

    # Determine the number of pages based on the number of listings per page (e.g., 24 listings per page)
    listings_per_page = 20
    last_page = math.ceil(total_listings / listings_per_page)

        #return last_page


    # Set the desired execution time to one hour (3600 seconds)
    execution_time = time.time() + 3000

    start_page, start_province  = get_last_scraped_page_and_year()

    execution_time = time.time() + 3000
    page=start_page
    province=start_province
    #last_page = get_last_page(province)

    provinces_to_scrape = [1,1,1,1,1,1,1,1,1,1,1,1]

    for province in provinces_to_scrape: 
        last_page = 660
        for page in range(start_page, last_page + 1):
            response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-{province}?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")

            home_page = BeautifulSoup(response.content, 'lxml')



            # Find all the car listings on the page 
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})



            for each_div in cars_containers:
                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    if time.time() >= execution_time :
                        execution_time += 120
                        time.sleep(60)
                    try:
                        found_link = (base_url + link['href'])
                        Car_ID = re.search(r'/(\d+)\?', found_link).group(1)
                    except:
                         continue

                    try:
                        # Get the HTML content of the car listing page
                        res = requests.get(found_link, timeout=10)
                                            # Assuming 'res.content' contains your HTML content

                        # Assuming 'res.content' contains your HTML content
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

                        # Parse the cleaned HTML content
                        car_soup = BeautifulSoup(cleaned_html, 'lxml')
                        # Step : Extract data from the car listing page
                        title = car_soup.find(class_='e-listing-title').text.strip()



                        ul = car_soup.find('ul', class_='b-breadcrumbs')
                        li_items = ul.find_all('li')



                        #car_id = li_items[6].find('span', itemprop='name').text.strip()
                        location = li_items[2].text.strip()
                        brand = li_items[3].text.strip()
                        model = li_items[4].text.strip()
                        variant = li_items[5].text.strip()

                        # Find the <div class="b-rate-container"> element
                        rating_container = car_soup.find('div', class_='b-rate-container')
                        # Find the <div class="col-8"> element within the rating_container
                        suburb_div = rating_container.find_next('div', class_='col-8')
                        # Find the <span> element within the suburb_div
                        suburb_element = suburb_div.find('span')
                        if suburb_element:
                            suburb = suburb_element.get_text(strip=True)
                        else:
                            suburb = None                        



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



                        last_updated = car_soup.find(text='Last Updated').find_next('div').text.strip() if car_soup.find(text='Last Updated') else None
                        previous_owners = car_soup.find(text='Previous Owners').find_next('div').text.strip() if car_soup.find(text='Previous Owners') else None
                        manufacturers_colour = car_soup.find(text='Colour').find_next('div').text.strip() if car_soup.find(text='Colour') else None
                        if manufacturers_colour == None:
                            manufacturers_colour = car_soup.find(text='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(text='Manufacturers Colour') else None
                        body_type = car_soup.find(text='Body Type').find_next('div').text.strip() if car_soup.find(text='Body Type') else None
                        warranty_remaining = car_soup.find(text='Warranty Remaining').find_next('div').text.strip() if car_soup.find(text='Warranty Remaining') else None
                        service_history = car_soup.find(text='Service History').find_next('div').text.strip() if car_soup.find(text='Service History') else None




                        introduction_date = car_soup.find(text='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Introduction date') else None
                        end_date = car_soup.find(text='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='End date') else None
                        service_interval_distance = car_soup.find(text='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Service interval distance') else None




                        engine_position = car_soup.find(text='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine position') else None
                        engine_detail = car_soup.find(text='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine detail') else None
                        engine_capacity = car_soup.find(text='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine capacity (litre)') else None
                        cylinder_layout = car_soup.find(text='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Cylinder layout and quantity') else None
                        fuel_type_engine = car_soup.find(text='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel type') else None
                        fuel_capacity = car_soup.find(text='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel capacity') else None
                        fuel_consumption = car_soup.find(text='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel consumption (average)') else None
                        fuel_range = car_soup.find(text='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel range (average)') else None
                        power_max = car_soup.find(text='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Power maximum (detail)') else None
                        torque_max = car_soup.find(text='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Torque maximum') else None
                        acceleration = car_soup.find(text='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Acceleration 0-100 km/h') else None
                        maximum_speed = car_soup.find(text='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Maximum/top speed') else None
                        co2_emissions = car_soup.find(text='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='CO2 emissions (average)') else None
                        #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        current_datetime = datetime.now().strftime('%Y-%m-%d')



                        province_names = {
                            1: 'Gauteng',
                            2: 'Kwazulu Natal',
                            3: 'Free State',
                            5: 'Mpumalanga',
                            6: 'North West',
                            7: 'Eastern Cape',
                            8: 'Northern Cape',
                            9: 'Western Cape',
                            14: 'Limpopo'
                        }
                        province_name = province_names.get(province)



                        car_data = {
                            'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                            'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                            'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                            'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                            'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                            'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                            'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                            'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                            'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name
                        }                      



                    except ConnectTimeout:
                        print(f"Connection timed out for link: {found_link}")
                        continue



                    car_data['Car_ID'] = Car_ID
                    car_data['DealerUrl']=DealerUrl
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
                        cleaned_column = [x.replace('.', ',') if x is not None else x for x in cleaned_column]
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

                    cursor.execute(
                        f"INSERT INTO {table_name} (Title, Car_ID, Region, Make, Model, Variant, Suburb,Province, Price, ExpectedPaymentPerMonth, CarType, RegistrationYear, Mileage, Transmission, FuelType, PriceRating, Dealer, LastUpdated, PreviousOwners, ManufacturersColour, BodyType, ServiceHistory, WarrantyRemaining, IntroductionDate, EndDate, ServiceIntervalDistance, EnginePosition, EngineDetail, EngineCapacity, CylinderLayoutAndQuantity, FuelTypeEngine, FuelCapacity, FuelConsumption, FuelRange, PowerMaximum, TorqueMaximum, Acceleration, MaximumSpeed, CO2Emissions, Version, DealerUrl, Timestamp) VALUES ( ?,?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        car_data['title'], car_data['Car_ID'], car_data['location'], car_data['brand'], car_data['model'], car_data['variant'], car_data['suburb'], car_data['province_name'],car_data['price'], car_data['expected_payment'], car_data['car_type'], car_data['registration_year'],
                        car_data['mileage'], car_data['transmission'], car_data['fuel_type'], car_data['price_rating'], car_data['dealer'], car_data['last_updated'], car_data['previous_owners'], car_data['manufacturers_colour'],
                        car_data['body_type'], car_data['service_history'], car_data['warranty_remaining'], car_data['introduction_date'], car_data['end_date'], car_data['service_interval_distance'], car_data['engine_position'],
                        car_data['engine_detail'], car_data['engine_capacity'], car_data['cylinder_layout'], car_data['fuel_type_engine'], car_data['fuel_capacity'], car_data['fuel_consumption'],
                        car_data['fuel_range'], car_data['power_max'], car_data['torque_max'], car_data['acceleration'], car_data['maximum_speed'], car_data['co2_emissions'], car_data['DealerUrl'], car_data['current_datetime'])

                    # Commit the changes
                    conn.commit()
                    update_last_scraped_page_and_year(page,province)

        start_page = 440
        page=440
        update_last_scraped_page_and_year(page,province)    

    conn.close()    


def code_4():
    #!/usr/bin/env python
    # coding: utf-8

    import requests
    from bs4 import BeautifulSoup
    import random
    import pyodbc
    import re
    import time
    import datetime
    from datetime import datetime
    from requests.exceptions import ConnectTimeout
    import math
    # URL of the Autotrader website
    base_url = "https://www.autotrader.co.za"

    # define the connection details
    server = 'web-development.database.windows.net'
    database = 'graduates'
    username = 'Canvas'
    password = 'Dut950505'
    driver = '{ODBC Driver 17 for SQL Server}'

    # create a connection
    conn = pyodbc.connect(
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'DRIVER={driver};')

    # create a cursor
    cursor = conn.cursor()

    # Step : Define the table name
    table_name = 'l_All'
    # Check if the table exists, if not create it
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ( Car_ID NVARCHAR(50),Title NVARCHAR(MAX), Region NVARCHAR(100), Make NVARCHAR(100), Model NVARCHAR(100), Variant NVARCHAR(100), Suburb NVARCHAR(100), Province NVARCHAR (50),Price NVARCHAR(100), ExpectedPaymentPerMonth NVARCHAR(100), CarType NVARCHAR(100), RegistrationYear NVARCHAR(100), Mileage NVARCHAR(100), Transmission NVARCHAR(100), FuelType NVARCHAR(100), PriceRating NVARCHAR(100), Dealer NVARCHAR(100), LastUpdated NVARCHAR(100), PreviousOwners NVARCHAR(100), ManufacturersColour NVARCHAR(100), BodyType NVARCHAR(100),ServiceHistory NVARCHAR(100), WarrantyRemaining NVARCHAR(100), IntroductionDate NVARCHAR(100), EndDate NVARCHAR(100), ServiceIntervalDistance NVARCHAR(100), EnginePosition NVARCHAR(100), EngineDetail NVARCHAR(100), EngineCapacity NVARCHAR(100), CylinderLayoutAndQuantity NVARCHAR(100), FuelTypeEngine NVARCHAR(100), FuelCapacity NVARCHAR(100), FuelConsumption NVARCHAR(100), FuelRange NVARCHAR(100), PowerMaximum NVARCHAR(100), TorqueMaximum NVARCHAR(100), Acceleration NVARCHAR(100), MaximumSpeed NVARCHAR(100), CO2Emissions NVARCHAR(100), Version INT,DealerUrl NVARCHAR(100), Timestamp DATETIME)")
    # Commit the changes and close the connection
    conn.commit()


    # Function to get the last scraped page and province
    def get_last_scraped_page_and_year():
        cursor = conn.cursor()
        select_query = "SELECT page, province FROM autotpage WHERE id =22" 
        cursor.execute(select_query)
        record = cursor.fetchone()
        if record is not None:
            return record.page, record.province
        else:
            return 660, 1  # Default values if the record doesn't exist

    def update_last_scraped_page_and_year(page,province):
        cursor = conn.cursor()
        update_query = "UPDATE autotpage SET page = ?, province = ? WHERE id =22"
        values = (page, province)
        cursor.execute(update_query, values)
        conn.commit()

    # Set the desired execution time to one hour (3600 seconds)
    execution_time = time.time() + 3000

    start_page, start_province  = get_last_scraped_page_and_year()

    execution_time = time.time() + 3000
    page=start_page
    province=start_province
    #last_page = get_last_page(province)

    provinces_to_scrape = [1,1,1,1,1,1,1,1,1,1,1,1]

    for province in provinces_to_scrape: 
        last_page = 880
        for page in range(start_page, last_page + 1):
            response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-{province}?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")

            home_page = BeautifulSoup(response.content, 'lxml')



            # Find all the car listings on the page 
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})



            for each_div in cars_containers:
                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    if time.time() >= execution_time :
                        execution_time += 120
                        time.sleep(60)
                    try:
                        found_link = (base_url + link['href'])
                        Car_ID = re.search(r'/(\d+)\?', found_link).group(1)
                    except:
                         continue

                    try:
                        # Get the HTML content of the car listing page
                        res = requests.get(found_link, timeout=10)
                                            # Assuming 'res.content' contains your HTML content

                        # Assuming 'res.content' contains your HTML content
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

                        # Parse the cleaned HTML content
                        car_soup = BeautifulSoup(cleaned_html, 'lxml')
                        # Step : Extract data from the car listing page
                        title = car_soup.find(class_='e-listing-title').text.strip()



                        ul = car_soup.find('ul', class_='b-breadcrumbs')
                        li_items = ul.find_all('li')



                        #car_id = li_items[6].find('span', itemprop='name').text.strip()
                        location = li_items[2].text.strip()
                        brand = li_items[3].text.strip()
                        model = li_items[4].text.strip()
                        variant = li_items[5].text.strip()

                        # Find the <div class="b-rate-container"> element
                        rating_container = car_soup.find('div', class_='b-rate-container')
                        # Find the <div class="col-8"> element within the rating_container
                        suburb_div = rating_container.find_next('div', class_='col-8')
                        # Find the <span> element within the suburb_div
                        suburb_element = suburb_div.find('span')
                        if suburb_element:
                            suburb = suburb_element.get_text(strip=True)
                        else:
                            suburb = None                        



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
                            car_data['DealerUrl']=DealerUrl



                        last_updated = car_soup.find(text='Last Updated').find_next('div').text.strip() if car_soup.find(text='Last Updated') else None
                        previous_owners = car_soup.find(text='Previous Owners').find_next('div').text.strip() if car_soup.find(text='Previous Owners') else None
                        manufacturers_colour = car_soup.find(text='Colour').find_next('div').text.strip() if car_soup.find(text='Colour') else None
                        if manufacturers_colour == None:
                            manufacturers_colour = car_soup.find(text='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(text='Manufacturers Colour') else None
                        body_type = car_soup.find(text='Body Type').find_next('div').text.strip() if car_soup.find(text='Body Type') else None
                        warranty_remaining = car_soup.find(text='Warranty Remaining').find_next('div').text.strip() if car_soup.find(text='Warranty Remaining') else None
                        service_history = car_soup.find(text='Service History').find_next('div').text.strip() if car_soup.find(text='Service History') else None




                        introduction_date = car_soup.find(text='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Introduction date') else None
                        end_date = car_soup.find(text='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='End date') else None
                        service_interval_distance = car_soup.find(text='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Service interval distance') else None




                        engine_position = car_soup.find(text='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine position') else None
                        engine_detail = car_soup.find(text='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine detail') else None
                        engine_capacity = car_soup.find(text='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine capacity (litre)') else None
                        cylinder_layout = car_soup.find(text='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Cylinder layout and quantity') else None
                        fuel_type_engine = car_soup.find(text='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel type') else None
                        fuel_capacity = car_soup.find(text='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel capacity') else None
                        fuel_consumption = car_soup.find(text='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel consumption (average)') else None
                        fuel_range = car_soup.find(text='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel range (average)') else None
                        power_max = car_soup.find(text='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Power maximum (detail)') else None
                        torque_max = car_soup.find(text='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Torque maximum') else None
                        acceleration = car_soup.find(text='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Acceleration 0-100 km/h') else None
                        maximum_speed = car_soup.find(text='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Maximum/top speed') else None
                        co2_emissions = car_soup.find(text='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='CO2 emissions (average)') else None
                        #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        current_datetime = datetime.now().strftime('%Y-%m-%d')



                        province_names = {
                            1: 'Gauteng',
                            2: 'Kwazulu Natal',
                            3: 'Free State',
                            5: 'Mpumalanga',
                            6: 'North West',
                            7: 'Eastern Cape',
                            8: 'Northern Cape',
                            9: 'Western Cape',
                            14: 'Limpopo'
                        }
                        province_name = province_names.get(province)



                        car_data = {
                            'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                            'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                            'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                            'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                            'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                            'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                            'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                            'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                            'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name
                        }                     



                    except ConnectTimeout:
                        print(f"Connection timed out for link: {found_link}")
                        continue



                    car_data['Car_ID'] = Car_ID
                    car_data['DealerUrl']=DealerUrl
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
                        cleaned_column = [x.replace('.', ',') if x is not None else x for x in cleaned_column]
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


                    cursor.execute(
                        f"INSERT INTO {table_name} (Title, Car_ID, Region, Make, Model, Variant, Suburb,Province, Price, ExpectedPaymentPerMonth, CarType, RegistrationYear, Mileage, Transmission, FuelType, PriceRating, Dealer, LastUpdated, PreviousOwners, ManufacturersColour, BodyType, ServiceHistory, WarrantyRemaining, IntroductionDate, EndDate, ServiceIntervalDistance, EnginePosition, EngineDetail, EngineCapacity, CylinderLayoutAndQuantity, FuelTypeEngine, FuelCapacity, FuelConsumption, FuelRange, PowerMaximum, TorqueMaximum, Acceleration, MaximumSpeed, CO2Emissions, Version, DealerUrl, Timestamp) VALUES ( ?,?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        car_data['title'], car_data['Car_ID'], car_data['location'], car_data['brand'], car_data['model'], car_data['variant'], car_data['suburb'], car_data['province_name'],car_data['price'], car_data['expected_payment'], car_data['car_type'], car_data['registration_year'],
                        car_data['mileage'], car_data['transmission'], car_data['fuel_type'], car_data['price_rating'], car_data['dealer'], car_data['last_updated'], car_data['previous_owners'], car_data['manufacturers_colour'],
                        car_data['body_type'], car_data['service_history'], car_data['warranty_remaining'], car_data['introduction_date'], car_data['end_date'], car_data['service_interval_distance'], car_data['engine_position'],
                        car_data['engine_detail'], car_data['engine_capacity'], car_data['cylinder_layout'], car_data['fuel_type_engine'], car_data['fuel_capacity'], car_data['fuel_consumption'],
                        car_data['fuel_range'], car_data['power_max'], car_data['torque_max'], car_data['acceleration'], car_data['maximum_speed'], car_data['co2_emissions'], car_data['DealerUrl'], car_data['current_datetime'])

                    # Commit the changes
                    conn.commit()
                    update_last_scraped_page_and_year(page,province)   
        

        start_page = 660
        page=660
        update_last_scraped_page_and_year(page,province)    

    conn.close()     

def code_5():
    # Your code for task 2
    #!/usr/bin/env python
    # coding: utf-8

    import requests
    from bs4 import BeautifulSoup
    import random
    import pyodbc
    import re
    import time
    import datetime
    from datetime import datetime
    from requests.exceptions import ConnectTimeout
    import math
    # URL of the Autotrader website
    base_url = "https://www.autotrader.co.za"

    # define the connection details
    server = 'web-development.database.windows.net'
    database = 'graduates'
    username = 'Canvas'
    password = 'Dut950505'
    driver = '{ODBC Driver 17 for SQL Server}'

    # create a connection
    conn = pyodbc.connect(
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'DRIVER={driver};')

    # create a cursor
    cursor = conn.cursor()

    # Step : Define the table name
    table_name = 'l_All'
    # Check if the table exists, if not create it
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ( Car_ID NVARCHAR(50),Title NVARCHAR(MAX), Region NVARCHAR(100), Make NVARCHAR(100), Model NVARCHAR(100), Variant NVARCHAR(100), Suburb NVARCHAR(100), Province NVARCHAR (50),Price NVARCHAR(100), ExpectedPaymentPerMonth NVARCHAR(100), CarType NVARCHAR(100), RegistrationYear NVARCHAR(100), Mileage NVARCHAR(100), Transmission NVARCHAR(100), FuelType NVARCHAR(100), PriceRating NVARCHAR(100), Dealer NVARCHAR(100), LastUpdated NVARCHAR(100), PreviousOwners NVARCHAR(100), ManufacturersColour NVARCHAR(100), BodyType NVARCHAR(100),ServiceHistory NVARCHAR(100), WarrantyRemaining NVARCHAR(100), IntroductionDate NVARCHAR(100), EndDate NVARCHAR(100), ServiceIntervalDistance NVARCHAR(100), EnginePosition NVARCHAR(100), EngineDetail NVARCHAR(100), EngineCapacity NVARCHAR(100), CylinderLayoutAndQuantity NVARCHAR(100), FuelTypeEngine NVARCHAR(100), FuelCapacity NVARCHAR(100), FuelConsumption NVARCHAR(100), FuelRange NVARCHAR(100), PowerMaximum NVARCHAR(100), TorqueMaximum NVARCHAR(100), Acceleration NVARCHAR(100), MaximumSpeed NVARCHAR(100), CO2Emissions NVARCHAR(100), Version INT,DealerUrl NVARCHAR(100), Timestamp DATETIME)")
    # Commit the changes and close the connection
    conn.commit()


    def get_last_scraped_page_and_year():
        cursor = conn.cursor()
        select_query = "SELECT page, province FROM autotpage WHERE id =23" 
        cursor.execute(select_query)
        record = cursor.fetchone()
        if record is not None:
            return record.page, record.province
        else:
            return 880, 1  # Default values if the record doesn't exist

    def update_last_scraped_page_and_year(page,province):
        cursor = conn.cursor()
        update_query = "UPDATE autotpage SET page = ?, province = ? WHERE id =23"
        values = (page, province)
        cursor.execute(update_query, values)
        conn.commit()  

    # Function to get the last page for a specific year
    #def get_last_page(province):
        #response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/kwazulu-natal/p-{province}")

        #home_page = BeautifulSoup(response.content, 'lxml')

        #total_listings_element = home_page.find('span', class_='e-results-total')
        #total_listings = int(total_listings_element.text.replace(' ', ''))

        # Determine the number of pages based on the number of listings per page (e.g., 24 listings per page)
        #listings_per_page = 20
        #last_page = math.ceil(total_listings / listings_per_page)

        #return last_page


    # Set the desired execution time to one hour (3600 seconds)
    execution_time = time.time() + 3000

    start_page, start_province  = get_last_scraped_page_and_year()

    execution_time = time.time() + 3000
    page=start_page
    province=start_province
    #last_page = get_last_page(province)

    provinces_to_scrape = [1,1,1,1,1,1,1,1,1,1,1,1]

    for province in provinces_to_scrape: 
        last_page = 1320
        for page in range(start_page, last_page + 1):
            response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-{province}?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")

            home_page = BeautifulSoup(response.content, 'lxml')


            # Find all the car listings on the page 
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})



            for each_div in cars_containers:
                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    if time.time() >= execution_time :
                        execution_time += 120
                        time.sleep(60)
                    try:
                        found_link = (base_url + link['href'])
                        Car_ID = re.search(r'/(\d+)\?', found_link).group(1)
                    except:
                         continue

                    try:
                        # Get the HTML content of the car listing page
                        res = requests.get(found_link, timeout=10)
                                            # Assuming 'res.content' contains your HTML content

                        # Assuming 'res.content' contains your HTML content
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

                        # Parse the cleaned HTML content
                        car_soup = BeautifulSoup(cleaned_html, 'lxml')
                        # Step : Extract data from the car listing page
                        title = car_soup.find(class_='e-listing-title').text.strip()



                        ul = car_soup.find('ul', class_='b-breadcrumbs')
                        li_items = ul.find_all('li')



                        #car_id = li_items[6].find('span', itemprop='name').text.strip()
                        location = li_items[2].text.strip()
                        brand = li_items[3].text.strip()
                        model = li_items[4].text.strip()
                        variant = li_items[5].text.strip()

                        # Find the <div class="b-rate-container"> element
                        rating_container = car_soup.find('div', class_='b-rate-container')
                        # Find the <div class="col-8"> element within the rating_container
                        suburb_div = rating_container.find_next('div', class_='col-8')
                        # Find the <span> element within the suburb_div
                        suburb_element = suburb_div.find('span')
                        if suburb_element:
                            suburb = suburb_element.get_text(strip=True)
                        else:
                            suburb = None                        



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



                        last_updated = car_soup.find(text='Last Updated').find_next('div').text.strip() if car_soup.find(text='Last Updated') else None
                        previous_owners = car_soup.find(text='Previous Owners').find_next('div').text.strip() if car_soup.find(text='Previous Owners') else None
                        manufacturers_colour = car_soup.find(text='Colour').find_next('div').text.strip() if car_soup.find(text='Colour') else None
                        if manufacturers_colour == None:
                            manufacturers_colour = car_soup.find(text='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(text='Manufacturers Colour') else None
                        body_type = car_soup.find(text='Body Type').find_next('div').text.strip() if car_soup.find(text='Body Type') else None
                        warranty_remaining = car_soup.find(text='Warranty Remaining').find_next('div').text.strip() if car_soup.find(text='Warranty Remaining') else None
                        service_history = car_soup.find(text='Service History').find_next('div').text.strip() if car_soup.find(text='Service History') else None




                        introduction_date = car_soup.find(text='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Introduction date') else None
                        end_date = car_soup.find(text='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='End date') else None
                        service_interval_distance = car_soup.find(text='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Service interval distance') else None




                        engine_position = car_soup.find(text='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine position') else None
                        engine_detail = car_soup.find(text='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine detail') else None
                        engine_capacity = car_soup.find(text='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine capacity (litre)') else None
                        cylinder_layout = car_soup.find(text='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Cylinder layout and quantity') else None
                        fuel_type_engine = car_soup.find(text='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel type') else None
                        fuel_capacity = car_soup.find(text='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel capacity') else None
                        fuel_consumption = car_soup.find(text='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel consumption (average)') else None
                        fuel_range = car_soup.find(text='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel range (average)') else None
                        power_max = car_soup.find(text='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Power maximum (detail)') else None
                        torque_max = car_soup.find(text='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Torque maximum') else None
                        acceleration = car_soup.find(text='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Acceleration 0-100 km/h') else None
                        maximum_speed = car_soup.find(text='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Maximum/top speed') else None
                        co2_emissions = car_soup.find(text='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='CO2 emissions (average)') else None
                        #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        current_datetime = datetime.now().strftime('%Y-%m-%d')



                        province_names = {
                            1: 'Gauteng',
                            2: 'Kwazulu Natal',
                            3: 'Free State',
                            5: 'Mpumalanga',
                            6: 'North West',
                            7: 'Eastern Cape',
                            8: 'Northern Cape',
                            9: 'Western Cape',
                            14: 'Limpopo'
                        }
                        province_name = province_names.get(province)



                        car_data = {
                            'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                            'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                            'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                            'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                            'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                            'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                            'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                            'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                            'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name
                        }                      



                    except ConnectTimeout:
                        print(f"Connection timed out for link: {found_link}")
                        continue



                    car_data['Car_ID'] = Car_ID
                    car_data['DealerUrl']=DealerUrl
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
                        cleaned_column = [x.replace('.', ',') if x is not None else x for x in cleaned_column]
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


                    cursor.execute(
                        f"INSERT INTO {table_name} (Title, Car_ID, Region, Make, Model, Variant, Suburb,Province, Price, ExpectedPaymentPerMonth, CarType, RegistrationYear, Mileage, Transmission, FuelType, PriceRating, Dealer, LastUpdated, PreviousOwners, ManufacturersColour, BodyType, ServiceHistory, WarrantyRemaining, IntroductionDate, EndDate, ServiceIntervalDistance, EnginePosition, EngineDetail, EngineCapacity, CylinderLayoutAndQuantity, FuelTypeEngine, FuelCapacity, FuelConsumption, FuelRange, PowerMaximum, TorqueMaximum, Acceleration, MaximumSpeed, CO2Emissions, Version, DealerUrl, Timestamp) VALUES ( ?,?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        car_data['title'], car_data['Car_ID'], car_data['location'], car_data['brand'], car_data['model'], car_data['variant'], car_data['suburb'], car_data['province_name'],car_data['price'], car_data['expected_payment'], car_data['car_type'], car_data['registration_year'],
                        car_data['mileage'], car_data['transmission'], car_data['fuel_type'], car_data['price_rating'], car_data['dealer'], car_data['last_updated'], car_data['previous_owners'], car_data['manufacturers_colour'],
                        car_data['body_type'], car_data['service_history'], car_data['warranty_remaining'], car_data['introduction_date'], car_data['end_date'], car_data['service_interval_distance'], car_data['engine_position'],
                        car_data['engine_detail'], car_data['engine_capacity'], car_data['cylinder_layout'], car_data['fuel_type_engine'], car_data['fuel_capacity'], car_data['fuel_consumption'],
                        car_data['fuel_range'], car_data['power_max'], car_data['torque_max'], car_data['acceleration'], car_data['maximum_speed'], car_data['co2_emissions'], car_data['DealerUrl'], car_data['current_datetime'])

                    # Commit the changes
                    conn.commit()
                    update_last_scraped_page_and_year(page,province)


        start_page = 880
        page=880
        update_last_scraped_page_and_year(page,province)    

    conn.close()  

def code_6():
    # Your code for task 3
    import requests
    from bs4 import BeautifulSoup
    import random
    import pyodbc
    import re
    import time
    import datetime
    from datetime import datetime
    from requests.exceptions import ConnectTimeout
    import math
    # URL of the Autotrader website
    base_url = "https://www.autotrader.co.za"

    # define the connection details
    server = 'web-development.database.windows.net'
    database = 'graduates'
    username = 'Canvas'
    password = 'Dut950505'
    driver = '{ODBC Driver 17 for SQL Server}'

    # create a connection
    conn = pyodbc.connect(
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          f'DRIVER={driver};')

    # create a cursor
    cursor = conn.cursor()

    # Step : Define the table name
    table_name = 'l_All'
    # Check if the table exists, if not create it
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ( Car_ID NVARCHAR(50),Title NVARCHAR(MAX), Region NVARCHAR(100), Make NVARCHAR(100), Model NVARCHAR(100), Variant NVARCHAR(100), Suburb NVARCHAR(100), Province NVARCHAR (50),Price NVARCHAR(100), ExpectedPaymentPerMonth NVARCHAR(100), CarType NVARCHAR(100), RegistrationYear NVARCHAR(100), Mileage NVARCHAR(100), Transmission NVARCHAR(100), FuelType NVARCHAR(100), PriceRating NVARCHAR(100), Dealer NVARCHAR(100), LastUpdated NVARCHAR(100), PreviousOwners NVARCHAR(100), ManufacturersColour NVARCHAR(100), BodyType NVARCHAR(100),ServiceHistory NVARCHAR(100), WarrantyRemaining NVARCHAR(100), IntroductionDate NVARCHAR(100), EndDate NVARCHAR(100), ServiceIntervalDistance NVARCHAR(100), EnginePosition NVARCHAR(100), EngineDetail NVARCHAR(100), EngineCapacity NVARCHAR(100), CylinderLayoutAndQuantity NVARCHAR(100), FuelTypeEngine NVARCHAR(100), FuelCapacity NVARCHAR(100), FuelConsumption NVARCHAR(100), FuelRange NVARCHAR(100), PowerMaximum NVARCHAR(100), TorqueMaximum NVARCHAR(100), Acceleration NVARCHAR(100), MaximumSpeed NVARCHAR(100), CO2Emissions NVARCHAR(100), Version INT,DealerUrl NVARCHAR(100), Timestamp DATETIME)")
    # Commit the changes and close the connection
    conn.commit()


    def get_last_scraped_page_and_year():
        cursor = conn.cursor()
        select_query = "SELECT page, province FROM autotpage WHERE id =24" 
        cursor.execute(select_query)
        record = cursor.fetchone()
        if record is not None:
            return record.page, record.province
        else:
            return 1320, 1  # Default values if the record doesn't exist

    def update_last_scraped_page_and_year(page,province):
        cursor = conn.cursor()
        update_query = "UPDATE autotpage SET page = ?, province = ? WHERE id =24"
        values = (page, province)
        cursor.execute(update_query, values)
        conn.commit() 

    # Function to get the last page for a specific year
    #def get_last_page(province):
    response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/kwazulu-natal/p-1")

    home_page = BeautifulSoup(response.content, 'lxml')

    total_listings_element = home_page.find('span', class_='e-results-total')
    total_listings = int(total_listings_element.text.replace(' ', ''))

    # Determine the number of pages based on the number of listings per page (e.g., 24 listings per page)
    listings_per_page = 20
    last_page = math.ceil(total_listings / listings_per_page)

        #return last_page


    # Set the desired execution time to one hour (3600 seconds)
    execution_time = time.time() + 3000

    start_page, start_province  = get_last_scraped_page_and_year()

    execution_time = time.time() + 3000
    page=start_page
    province=start_province
    #last_page = get_last_page(province)

    provinces_to_scrape = [1,1,1,1,1,1,1,1,1,1,1,1]

    for province in provinces_to_scrape: 
        last_page = 1540
        for page in range(start_page, last_page + 1):
            response = requests.get(f"https://www.autotrader.co.za/cars-for-sale/gauteng/p-{province}?pagenumber={page}&sortorder=Newest&priceoption=RetailPrice")

            home_page = BeautifulSoup(response.content, 'lxml')



            # Find all the car listings on the page 
            cars_containers = home_page.find_all('div', attrs={'class': re.compile(r'b-result-tile .*')})



            for each_div in cars_containers:
                # Find the link to the car listing
                for link in each_div.find_all('a', href=True):
                    if time.time() >= execution_time :
                        execution_time += 120
                        time.sleep(60)
                    try:
                        found_link = (base_url + link['href'])
                        Car_ID = re.search(r'/(\d+)\?', found_link).group(1)
                    except:
                         continue

                    try:
                        # Get the HTML content of the car listing page
                        res = requests.get(found_link, timeout=10)
                                            # Assuming 'res.content' contains your HTML content

                        # Assuming 'res.content' contains your HTML content
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

                        # Parse the cleaned HTML content
                        car_soup = BeautifulSoup(cleaned_html, 'lxml')
                        # Step : Extract data from the car listing page
                        title = car_soup.find(class_='e-listing-title').text.strip()



                        ul = car_soup.find('ul', class_='b-breadcrumbs')
                        li_items = ul.find_all('li')



                        #car_id = li_items[6].find('span', itemprop='name').text.strip()
                        location = li_items[2].text.strip()
                        brand = li_items[3].text.strip()
                        model = li_items[4].text.strip()
                        variant = li_items[5].text.strip()

                        # Find the <div class="b-rate-container"> element
                        rating_container = car_soup.find('div', class_='b-rate-container')
                        # Find the <div class="col-8"> element within the rating_container
                        suburb_div = rating_container.find_next('div', class_='col-8')
                        # Find the <span> element within the suburb_div
                        suburb_element = suburb_div.find('span')
                        if suburb_element:
                            suburb = suburb_element.get_text(strip=True)
                        else:
                            suburb = None                        



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



                        last_updated = car_soup.find(text='Last Updated').find_next('div').text.strip() if car_soup.find(text='Last Updated') else None
                        previous_owners = car_soup.find(text='Previous Owners').find_next('div').text.strip() if car_soup.find(text='Previous Owners') else None
                        manufacturers_colour = car_soup.find(text='Colour').find_next('div').text.strip() if car_soup.find(text='Colour') else None
                        if manufacturers_colour == None:
                            manufacturers_colour = car_soup.find(text='Manufacturers Colour').find_next('div').text.strip() if car_soup.find(text='Manufacturers Colour') else None
                        body_type = car_soup.find(text='Body Type').find_next('div').text.strip() if car_soup.find(text='Body Type') else None
                        warranty_remaining = car_soup.find(text='Warranty Remaining').find_next('div').text.strip() if car_soup.find(text='Warranty Remaining') else None
                        service_history = car_soup.find(text='Service History').find_next('div').text.strip() if car_soup.find(text='Service History') else None




                        introduction_date = car_soup.find(text='Introduction date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Introduction date') else None
                        end_date = car_soup.find(text='End date').find_next('span', class_='col-6').text.strip() if car_soup.find(text='End date') else None
                        service_interval_distance = car_soup.find(text='Service interval distance').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Service interval distance') else None




                        engine_position = car_soup.find(text='Engine position').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine position') else None
                        engine_detail = car_soup.find(text='Engine detail').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine detail') else None
                        engine_capacity = car_soup.find(text='Engine capacity (litre)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Engine capacity (litre)') else None
                        cylinder_layout = car_soup.find(text='Cylinder layout and quantity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Cylinder layout and quantity') else None
                        fuel_type_engine = car_soup.find(text='Fuel type').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel type') else None
                        fuel_capacity = car_soup.find(text='Fuel capacity').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel capacity') else None
                        fuel_consumption = car_soup.find(text='Fuel consumption (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel consumption (average)') else None
                        fuel_range = car_soup.find(text='Fuel range (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Fuel range (average)') else None
                        power_max = car_soup.find(text='Power maximum (detail)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Power maximum (detail)') else None
                        torque_max = car_soup.find(text='Torque maximum').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Torque maximum') else None
                        acceleration = car_soup.find(text='Acceleration 0-100 km/h').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Acceleration 0-100 km/h') else None
                        maximum_speed = car_soup.find(text='Maximum/top speed').find_next('span', class_='col-6').text.strip() if car_soup.find(text='Maximum/top speed') else None
                        co2_emissions = car_soup.find(text='CO2 emissions (average)').find_next('span', class_='col-6').text.strip() if car_soup.find(text='CO2 emissions (average)') else None
                        #current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        current_datetime = datetime.now().strftime('%Y-%m-%d')



                        province_names = {
                            1: 'Gauteng',
                            2: 'Kwazulu Natal',
                            3: 'Free State',
                            5: 'Mpumalanga',
                            6: 'North West',
                            7: 'Eastern Cape',
                            8: 'Northern Cape',
                            9: 'Western Cape',
                            14: 'Limpopo'
                        }
                        province_name = province_names.get(province)



                        car_data = {
                            'title': title, 'location':location,'brand': brand,'model': model, 'variant':variant, 'suburb':suburb,
                            'price':price, 'expected_payment':expected_payment,'car_type': car_type,'registration_year': registration_year,
                            'mileage':mileage,'transmission': transmission, 'fuel_type':fuel_type, 'price_rating':price_rating, 
                            'dealer':dealer,'last_updated': last_updated, 'previous_owners':previous_owners,'manufacturers_colour': manufacturers_colour,
                            'body_type':body_type,'service_history':service_history, 'warranty_remaining':warranty_remaining,'introduction_date': introduction_date, 'end_date':end_date, 
                            'service_interval_distance':service_interval_distance,'engine_position': engine_position,'engine_detail':engine_detail, 
                            'engine_capacity':engine_capacity,'cylinder_layout': cylinder_layout, 'fuel_type_engine':fuel_type_engine, 'fuel_capacity':fuel_capacity, 
                            'fuel_consumption':fuel_consumption,'fuel_range': fuel_range,'power_max': power_max, 'torque_max':torque_max,  'acceleration':acceleration, 
                            'maximum_speed':maximum_speed, 'co2_emissions':co2_emissions, 'current_datetime':current_datetime,'province_name':province_name
                        }                      



                    except ConnectTimeout:
                        print(f"Connection timed out for link: {found_link}")
                        continue



                    car_data['Car_ID'] = Car_ID
                    car_data['DealerUrl']=DealerUrl
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
                        cleaned_column = [x.replace('.', ',') if x is not None else x for x in cleaned_column]
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

                    cursor.execute(
                        f"INSERT INTO {table_name} (Title, Car_ID, Region, Make, Model, Variant, Suburb,Province, Price, ExpectedPaymentPerMonth, CarType, RegistrationYear, Mileage, Transmission, FuelType, PriceRating, Dealer, LastUpdated, PreviousOwners, ManufacturersColour, BodyType, ServiceHistory, WarrantyRemaining, IntroductionDate, EndDate, ServiceIntervalDistance, EnginePosition, EngineDetail, EngineCapacity, CylinderLayoutAndQuantity, FuelTypeEngine, FuelCapacity, FuelConsumption, FuelRange, PowerMaximum, TorqueMaximum, Acceleration, MaximumSpeed, CO2Emissions, Version, DealerUrl, Timestamp) VALUES ( ?,?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        car_data['title'], car_data['Car_ID'], car_data['location'], car_data['brand'], car_data['model'], car_data['variant'], car_data['suburb'], car_data['province_name'],car_data['price'], car_data['expected_payment'], car_data['car_type'], car_data['registration_year'],
                        car_data['mileage'], car_data['transmission'], car_data['fuel_type'], car_data['price_rating'], car_data['dealer'], car_data['last_updated'], car_data['previous_owners'], car_data['manufacturers_colour'],
                        car_data['body_type'], car_data['service_history'], car_data['warranty_remaining'], car_data['introduction_date'], car_data['end_date'], car_data['service_interval_distance'], car_data['engine_position'],
                        car_data['engine_detail'], car_data['engine_capacity'], car_data['cylinder_layout'], car_data['fuel_type_engine'], car_data['fuel_capacity'], car_data['fuel_consumption'],
                        car_data['fuel_range'], car_data['power_max'], car_data['torque_max'], car_data['acceleration'], car_data['maximum_speed'], car_data['co2_emissions'], car_data['DealerUrl'], car_data['current_datetime'])

                    # Commit the changes
                    conn.commit()
                    update_last_scraped_page_and_year(page,province)

        start_page = 1320
        page=1320
        update_last_scraped_page_and_year(page,province)    

    conn.close()     
    
if __name__ == '__main__':
    thread_1 = threading.Thread(target=code_1)
    thread_2 = threading.Thread(target=code_2)
    thread_3 = threading.Thread(target=code_3)
    thread_4 = threading.Thread(target=code_4)
    thread_5 = threading.Thread(target=code_5)
    thread_6 = threading.Thread(target=code_6)

    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_4.start()
    thread_5.start()
    thread_6.start()
