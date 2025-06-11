import pandas as pd
import requests
import time
import re
import os
import gdown
from tqdm import tqdm

API_KEY = "AIzaSyDn5m6m-k27vFPpQPLiNYRgGE-2prFQv9k" # Best to use Railway ENV variable

# Step 1: File IDs for each part stored on your Google Drive
GDRIVE_FILE_IDS = {
    "1": "FILE_ID_FOR_PART_1",
    "2": "FILE_ID_FOR_PART_2",
    "3": "FILE_ID_FOR_PART_3",
    "4": "FILE_ID_FOR_PART_4",
    "5": "FILE_ID_FOR_PART_5"
}

def download_csv(part_number):
    file_id = GDRIVE_FILE_IDS.get(str(part_number))
    if not file_id:
        raise ValueError(f"No file ID found for part {part_number}")
    
    url = f"https://drive.google.com/uc?id={file_id}"
    output = f"input_part_{part_number}.csv"
    print(f"📥 Downloading part {part_number} from Google Drive...")
    gdown.download(url, output, quiet=False)
    print(f"✅ Downloaded to {output}")
    return output

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
    return {"Address": address, "PhoneNumber": phone, "Pincode": pincode}

def process_part(part_number):
    input_file = download_csv(part_number)
    output_file = f"output_part_{part_number}.parquet"

    df = pd.read_csv(input_file)
    df["Extracted_Address"] = ""
    df["PhoneNumber"] = ""
    df["Extracted_Pincode"] = ""
    df["Pincode_Match"] = ""

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
    print(f"✅ Done. Saved to {output_file}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        process_part(sys.argv[1])
    else:
        print("⚠️ Please run as: python process_part.py 1")
