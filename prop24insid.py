print("Buy inside code running......................")

#################################################getting runs

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import requests
from azure.storage.blob import BlobClient
from datetime import datetime, timedelta
import time
import math

from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobClient

def run_number():
    sas_url= f"https://stautotrader.blob.core.windows.net/run/run_number_prop242.csv?sv=2021-10-04&st=2023-10-16T16%3A15%3A06Z&se=2030-10-17T16%3A15%3A00Z&sr=c&sp=racwdxltf&sig=w00AJeguSxlLi%2F9%2FiCWZZd%2Bx8ZHy3GGQJOFawOy3im8%3D"

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
    sas_url= f"https://stautotrader.blob.core.windows.net/run/run_number_prop242.csv?sv=2021-10-04&st=2023-11-03T14%3A19%3A39Z&se=2050-11-04T14%3A19%3A00Z&sr=c&sp=racwdxltf&sig=vfN2PRe1%2BzQGdI2roDDgL12AGS4Vehu7my8K4FoVngk%3D"
    run_number_ = [0]

    pd_run = pd.DataFrame(run_number_)

    run_csv = pd_run.to_csv(index = False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(run_csv, overwrite=True)

def new_run_number():
    sas_url= f"https://stautotrader.blob.core.windows.net/run/run_number_prop242.csv?sv=2021-10-04&st=2023-11-03T14%3A19%3A39Z&se=2050-11-04T14%3A19%3A00Z&sr=c&sp=racwdxltf&sig=vfN2PRe1%2BzQGdI2roDDgL12AGS4Vehu7my8K4FoVngk%3D"
    
    run_number_1 = run_no + 1

    run_number_ = []

    run_number_.append(run_number_1)

    pd_run = pd.DataFrame(run_number_)

    run_csv = pd_run.to_csv(index=False, encoding = "utf-8")

    client = BlobClient.from_blob_url(sas_url)

    client.upload_blob(run_csv, overwrite=True)

#get run no
run_no=run_number()



def get_urls(run_no):
    sas_url = f"https://stautotrader.blob.core.windows.net/run/prop24_urls.csv?sv=2021-10-04&ss=btqf&srt=sco&st=2023-10-17T07%3A39%3A17Z&se=2030-10-18T07%3A39%3A00Z&sp=rwdxftlacup&sig=%2BTFZttmuMZLkl%2Bq%2Bf2t%2FPNBSJkWUzw52PPp1sL9X8Wk%3D"
    client = BlobClient.from_blob_url(sas_url)
    blob = client.download_blob()

    blob_content = blob.readall()
    # Use BytesIO to create a file-like object from the bytes
    data = BytesIO(blob_content)

    # Read the CSV from the file-like object
    data1 = pd.read_csv(data)

    # Select rows based on run_number
    start_row = (run_no - 1) * 200
    end_row = run_no * 200

    # Ensure that end_row does not exceed the total number of rows
    end_row = min(end_row, len(data1))

    # Select the first column for the specified range of rows
    selected_rows = data1.iloc[start_row:end_row, 0]
    return selected_rows



def get_maxrows():
    sas_url = f"https://stautotrader.blob.core.windows.net/run/prop24_urls.csv?sv=2021-10-04&ss=btqf&srt=sco&st=2023-10-17T07%3A39%3A17Z&se=2030-10-18T07%3A39%3A00Z&sp=rwdxftlacup&sig=%2BTFZttmuMZLkl%2Bq%2Bf2t%2FPNBSJkWUzw52PPp1sL9X8Wk%3D"
    client = BlobClient.from_blob_url(sas_url)
    blob = client.download_blob()

    blob_content = blob.readall()
    data = BytesIO(blob_content)
    data1 = pd.read_csv(data)
    
    lengthofdata=len(data1)
    return lengthofdata

if run_no==0:
    max_rows=2000
else:    
    max_rows=get_maxrows()
max_runs = math.ceil(max_rows / 200)


if run_no==0:
#######################################################extract urls
    import aiohttp
    import asyncio
    from bs4 import BeautifulSoup
    import datetime
    import pandas as pd
    import requests
    from azure.storage.blob import BlobClient
    from datetime import datetime, timedelta
    import time

    from datetime import datetime 
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)


    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5'
    }

    start_page = 1
    last_page = 200
    pages_per_batch = 101
    thread_data = []
    failed_pages = []  # List to store failed pages
    tile_urls_with_fake_class = []

    async def scrape_page(session, url, page):
        try:
            async with session.get(url, headers=headers, timeout=70) as response:
                if response.status == 200:
                    page_content = await response.text()
                    soup = BeautifulSoup(page_content, 'html.parser')

                    # Find all <style> tags with type="text/css"
                    style_tags = soup.find_all('style', type='text/css')

                    # Iterate over each style tag to find the class name after the <style type="text/css"> tag
                    fake_class_names = []
                    # Iterate over each style tag
                    for style_tag in style_tags:
                        # Extract the content of the style tag
                        style_content = style_tag.text.strip()
                        
                        # Split the content by lines and iterate over lines
                        for line in style_content.splitlines():
                            # Strip any leading/trailing whitespace
                            line = line.strip()
                            
                            # Check if the line starts with a '.' (indicating a class definition)
                            if line.startswith('.'):
                                # Extract the class name by splitting on '{' and taking the first part
                                class_name = line.split('{')[0].strip()
                                
                                # Remove the leading '.' from the class name
                                class_name = class_name[1:]  # Remove the leading '.'
                                
                                # Append the class name to the list
                                fake_class_names.append(class_name)
                                break

                    p24_results = soup.find('div', class_='p24_results')
                    if p24_results:
                        col_9_div = p24_results.find('div', class_='col-9')
                        if col_9_div:
                            tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
                            for tile in tile_containers:
                                                    
                                if any(cls in tile['class'] for cls in fake_class_names):
                                    a_tag = tile.find('a', href=True)
                                    url = a_tag['href'] if a_tag else None
                                    if url is not None:
                                        listing_number ="https://www.property24.com"+url
                                        tile_urls_with_fake_class.append(listing_number)
                                
                                else:                                
                                    a_tag = tile.find('a', href=True)
                                    url = a_tag['href'] if a_tag else None
                                    if url is not None:
                                        listing_number ="https://www.property24.com"+url
                                        thread_data.append(listing_number)                            

                else:
                    failed_pages.append(page)
        except Exception as e:
            failed_pages.append(page)

    async def scrape_batch(session, start, end):
        tasks = []
        for page in range(start, end + 1):
            url = f"https://www.property24.com/for-sale/advanced-search/results/p{page}?sp=pid%3d8%2c2%2c3%2c14%2c5%2c1%2c6%2c9%2c7%26so%3dNewest&PropertyCategory=House%2cApartmentOrFlat%2cTownhouse%2cVacantLandOrPlot%2cFarm%2cCommercial%2cIndustrial"
            tasks.append(scrape_page(session, url, page))
        await asyncio.gather(*tasks)

    async def main():
        async with aiohttp.ClientSession() as session:
            for start in range(start_page, last_page + 1, pages_per_batch):
                end = min(start + pages_per_batch - 1, last_page)
                await scrape_batch(session, start, end)
                if end < last_page:
                    await asyncio.sleep(60)
            global failed_pages
            if failed_pages:
                failed_pages = list(set(failed_pages))
                await scrape_batch(session, min(failed_pages), max(failed_pages))

    if __name__ == "__main__":

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        thread_data = [item for item in thread_data if item is not None]
        thread_data = list(set(thread_data))
        data_frame = pd.DataFrame(thread_data)

        # Save DataFrame to CSV
        car_data_csv = data_frame.to_csv(encoding="utf-8", index=False)
        sas_url= f"https://stautotrader.blob.core.windows.net/run/prop24_urls.csv?sv=2021-10-04&st=2023-10-16T16%3A15%3A06Z&se=2030-10-17T16%3A15%3A00Z&sr=c&sp=racwdxltf&sig=w00AJeguSxlLi%2F9%2FiCWZZd%2Bx8ZHy3GGQJOFawOy3im8%3D"
        client = BlobClient.from_blob_url(sas_url)
        client.upload_blob(car_data_csv, overwrite=True)  
        
        new_run_number()


##########################################EXTRACT DATA

if run_no>0:
    
    thread_data =get_urls(run_no)

    import aiohttp
    import asyncio
    import pandas as pd
    from bs4 import BeautifulSoup
    from datetime import datetime
    import time

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5'
    }

    base_url = "https://www.property24.com/for-sale/uvongo/margate/kwazulu-natal/6359/{id}"

    # Function to extract text from element or return None if not found
    def get_text_or_none(element):
        return element.get_text(strip=True) if element else None


    async def extract_property_details(session, listing_id):
        url = listing_id
        try:
            async with session.get(url, timeout=70) as response:
                if response.status != 200:
                    print(f"respomse status:{response.status}")
                    
                if response.status == 200:
                    page_content = await response.text()
                    soup = BeautifulSoup(page_content, 'html.parser')
                    
                    listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
                    photo_data = []
                    try:
                        photogrid_div = soup.find('div',class_='p24_mediaHolder hide').find('div',class_='p24_thumbnailContainer').find_all('div',class_='col-4 p24_galleryThumbnail')
                        if photogrid_div:
                            for x in photogrid_div:
                                photo_url = x.find('img').get('lazy-src')
                                photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
                    except:
                        print(f"No picture div found: {listing_id}")


                    listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
                    try:
                        comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
                        prop_desc = ' '.join(comment_24.stripped_strings)
                    except:
                        prop_desc = None
                    current_datetime = datetime.now().strftime('%Y-%m-%d')
                    data_desc= {"Listing ID": listing_id,
                                "Description": prop_desc,
                                "Time_stamp": current_datetime}
                    
                    Timestamp = datetime.now().strftime('%Y-%m-%d')
                    # Initialize dictionary to store property details
                    property_details = {
                        "Price": None,
                        "Title": None,
                        "Location": None,
                        "Bedrooms": None,
                        "Bathrooms": None,
                        "Parking Spaces": None,
                        "Floor Size": None,
                        "Garages": None,
                        "No Pets Allowed": None,
                        "Lifestyle": None,
                        "Erf Size": None,
                        "Price per m²": None,
                        "Pets Allowed": None,
                        "Furnished": None,
                        "Kitchen": None,
                        "Lounge": None,
                        "Garage": None,
                        "Security": None,
                        "Backup Water": None,
                        "Agency": None,
                        "Agency url": None,
                        "Listing Number": None,
                        "Type of Property": None,
                        "Listing Date": None,
                        "Levies": None,
                        "Rates and Taxes": None,
                        "Timestamp": Timestamp
                    }

                    # Extract main details
                    p24_results = soup.find('div', class_='p24_listingContent')

                    if p24_results:
                        tile_div = p24_results.find('div', class_='p24_listingFeaturesWrapper')
                        if tile_div:
                            p24_mBM_divs = tile_div.find_all('div', class_='p24_mBM')
                            if len(p24_mBM_divs) > 1 and len(p24_mBM_divs) == 2:
                                title_div = p24_mBM_divs[1]
                            else:
                                title_div = p24_mBM_divs[2]
                            property_details["Title"] = get_text_or_none(title_div.find('h1'))
                            property_details["Location"] = get_text_or_none(title_div.find_next_sibling('div', class_='p24_mBM'))
                            property_details["Timestamp"] = Timestamp

                        # Extract price
                        price_span = p24_results.find(class_='p24_price')
                        property_details["Price"] = get_text_or_none(price_span)

                        # Extract features
                        icons_wrapper = p24_results.find('div', class_='p24_iconsWrapper')
                        if icons_wrapper:
                            icons_list = icons_wrapper.find('ul', class_='p24_icons')
                            if icons_list:
                                feature_items = icons_list.find_all('li', class_='p24_featureDetails')
                                for item in feature_items:
                                    feature_title = item.get('title')
                                    feature_value = get_text_or_none(item.find('span'))
                                    if feature_title in property_details:
                                        property_details[feature_title] = feature_value

                        # Extract key features
                        key_features_containers = soup.find_all('div', class_='p24_keyFeaturesContainer')
                        for container in key_features_containers:
                            listing_features = container.find_all('div', class_='p24_listingFeatures')
                            for feature in listing_features:
                                feature_name = get_text_or_none(feature.find('span', class_='p24_feature'))
                                feature_value = get_text_or_none(feature.find('span', class_='p24_featureAmount'))
                                if feature_name in property_details:
                                    property_details[feature_name] = feature_value

                        # Extract property overview
                        property_overview = p24_results.find('div', class_='p24_listingCard p24_propertyOverview')

                        def extract_features(container):
                            features = {}
                            rows = container.find_all('div', class_='row p24_propertyOverviewRow')
                            for row in rows:
                                key = get_text_or_none(row.find('div', class_='p24_propertyOverviewKey'))
                                value_divs = row.find_all('div', class_='p24_info')
                                value = ", ".join([get_text_or_none(v) for v in value_divs])
                                features[key] = value
                            return features

                        if property_overview:
                            panels = property_overview.find_all('div', class_='panel')
                            for panel in panels:
                                heading = get_text_or_none(panel.find('div', class_='panel-heading'))
                                panel_body = panel.find('div', class_='panel-body')
                                if panel_body:
                                    features = extract_features(panel_body)
                                    for feature, value in features.items():
                                        if feature in property_details:
                                            property_details[feature] = value

                        # Extract agency name
                        agency_name_elem = soup.find('a', class_='p24_agencyLogoName')
                        property_details["Agency"] = get_text_or_none(agency_name_elem.find('span') if agency_name_elem else None)

                        # Extract agency URL
                        agency_url_elem = soup.find('a', class_='p24_agencyLogo')
                        property_details["Agency_url.1"] = "https://www.property24.com" + (agency_url_elem['href'] if agency_url_elem else '')

                    return photo_data,data_desc,property_details
                else:
                    return None
        except Exception as e:
            return None

    async def scrape_property_details(listing_ids):
        property_details_list = []  # List to hold all property details dictionaries
        prop_desc_list=[]
        photo_data_list=[]
        
        async with aiohttp.ClientSession() as session:
            for index, listing_id in enumerate(listing_ids, start=1):
                try:
                    #property_details = await extract_property_details(session, listing_id)
                    photo_data,prop_desc,  property_details = await extract_property_details(session, listing_id)
                    if property_details is not None:
                        property_details_list.append(property_details)
                        
                    if prop_desc is not None:
                        prop_desc_list.append(prop_desc)  
                        
                    if photo_data is not None:
                        photo_data_list.extend(photo_data)
                    
                except Exception as e:
                    pass           
                
                if index % 200 == 0 or index == len(listing_ids):
                    await asyncio.sleep(60)

        #ingest inside
        # Convert property details list to DataFrame
        df = pd.DataFrame(property_details_list)
        df.columns = df.columns.astype(str).str.replace(' ', '_')
        df.rename(columns={'Price_per_m²': 'Price_per_m2'}, inplace=True)
        # Save DataFrame to CSV
        car_data_csv = df.to_csv(encoding="utf-8", index=False)
        sas_url= f"https://stautotrader.blob.core.windows.net/prop24inside/property_details{run_no}.csv?sv=2023-01-03&st=2024-10-09T06%3A10%3A44Z&se=2026-09-10T06%3A10%3A00Z&sr=c&sp=racwdxltf&sig=ZvipYu0rU5p3ggJnrtCTMyjuu7ry9Edga3Blp%2FJCUsk%3D"
        client = BlobClient.from_blob_url(sas_url)
        client.upload_blob(car_data_csv, overwrite=True)          
        #df.to_csv('property_details.csv', index=False)

        #ingest comments and pics
        import time
        container_name = "comments-pics"
        timenow= datetime.now().strftime('%H:%M')
        filename_comments = f"Prop24Commentsgl{timenow}.csv"
        filename_pics = f"Prop24picsgl{timenow}.csv"
        
        data_frame_comments = pd.DataFrame(prop_desc_list)
        data_frame_photo = pd.DataFrame(photo_data_list)
        # Save DataFrame to CSV
        car_data_csv_com = data_frame_comments.to_csv(encoding="utf-8", index=False)
        car_data_csv_pic = data_frame_photo.to_csv(encoding="utf-8", index=False)
        connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
        client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=filename_comments)
        client.upload_blob(car_data_csv_com, overwrite=True)  
        client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=filename_pics)
        client.upload_blob(car_data_csv_pic, overwrite=True)      

        new_run_number()
        
        if run_no==max_runs:
            reset_run()
            quit()

    async def main():
        global thread_data

        # Scrape property details asynchronously
        await scrape_property_details(thread_data)

    if __name__ == "__main__":
        asyncio.run(main())

#trigger
if run_no<max_runs:
    token = 'glptt-e067c3c51ace8207de3e14d4f27ada4ca113ef53'
    response = requests.post(f'https://gitlab.com/api/v4/projects/62400499/ref/main/trigger/pipeline?token={token}')        
    
    
