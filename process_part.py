import pandas as pd
import requests
import time
import os
import re
from tqdm import tqdm

API_KEY = "AIzaSyDn5m6m-k27vFPpQPLiNYRgGE-2prFQv9k"

def search_place(company_name, address=None):
    query = f"{company_name}, {address}" if address else company_name
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={requests.utils.quote(query)}&key={API_KEY}"
    response = requests.get(url).json()
    if response.get('results'):
        return response['results'][0]['place_id']
    return None

def get_place_details(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,formatted_address,international_phone_number,formatted_phone_number&key={API_KEY}"
    response = requests.get(url).json()
    result = response.get('result', {})
    address = result.get('formatted_address', '')
    phone = result.get('international_phone_number', '') or result.get('formatted_phone_number', '')
    pincode_match = re.search(r'\b\d{6}\b', address)
    pincode = pincode_match.group() if pincode_match else ''
    return {
        "Address": address,
        "PhoneNumber": phone,
        "Pincode": pincode
    }

def process_part(part_number):
    input_file = f"input_part_{part_number}.csv"
    output_file = f"output_part_{part_number}.parquet"

    df = pd.read_csv(input_file)
    df['Extracted_Address'] = ''
    df['PhoneNumber'] = ''
    df['Extracted_Pincode'] = ''
    df['Pincode_Match'] = ''

    for index, row in tqdm(df.iterrows(), total=len(df)):
        company_name = str(row.get('EnterpriseName', '')).strip()
        address = str(row.get('CommunicationAddress', '')).strip()
        input_pincode = str(row.get('Pincode', '')).strip()

        if not company_name or not address:
            continue

        try:
            place_id = search_place(company_name, address)
            if place_id:
                details = get_place_details(place_id)
                df.at[index, 'Extracted_Address'] = details['Address']
                df.at[index, 'PhoneNumber'] = details['PhoneNumber']
                df.at[index, 'Extracted_Pincode'] = details['Pincode']
                df.at[index, 'Pincode_Match'] = str(details['Pincode']) == input_pincode
            time.sleep(1.2)
        except Exception as e:
            print(f"Error for {company_name}: {e}")
            continue

    df.to_parquet(output_file, index=False)
    print(f"Finished part {part_number} → {output_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        process_part(sys.argv[1])
    else:
        print("Please provide a part number (e.g., `python process_part.py 1`)")
