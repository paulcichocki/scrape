import requests
from bs4 import BeautifulSoup
import os
import argparse
import pandas as pd
import csv
import shutil

def download_link_from_url(url, download_folder, output_filename=None):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the first link (anchor tag) with the href attribute
        list_documents = soup.find(class_='list-documents')
        if list_documents:
            link = list_documents.find('a', href=True)
        else:
            link = None

        if link:
            download_url = link['href']
            
            # If the link is relative, make it absolute by combining with the base URL
            if not download_url.startswith('http'):
                download_url = requests.compat.urljoin(url, download_url)

            # Download the file with the specified or default name
            download_file(download_url, download_folder, output_filename)
        else:
            print('No downloadable link found within the "list-documents" class.')    
    except requests.exceptions.RequestException as e:
        print(f'Error occurred: {e}')

def download_file(download_url, download_folder, output_filename=None):
    try:
        # Send a GET request to the download URL
        response = requests.get(download_url, stream=True)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Use the specified output filename or extract the filename from the URL
        filename = output_filename if output_filename else os.path.basename(download_url)
        file_path = os.path.join(download_folder, filename)

        # Write the file content to disk
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        print(f'Downloaded: {file_path}')

        analyze(file_path, output_filename)
        
        shutil.rmtree(download_folder)
    except requests.exceptions.RequestException as e:
        print(f'Failed to download {download_url}: {e}')

def analyze(origin_file, output_file):
    try:
        # Read the Excel file into a DataFrame
        df = pd.read_excel(origin_file)

        # Ensure the 'GasDay' column is in datetime format
        df['GasDay'] = pd.to_datetime(df['GasDay'])

        # Set the output filename, defaulting to 'example.csv' if not provided
        filename = output_file if output_file else 'Sample.csv'

        # Prepare the data for CSV output
        data = []
        for index, row in df.iterrows():
            # Extract the date and values from the row
            col1 = row['GasDay'].date()
            col2 = float(row['D+1'])
            col3 = float(row['D+5'])

            # Append the row data to the list
            data.append([col1, col2, col3])

        # Write the data to a CSV file
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            # Write the header row
            writer.writerow(['GasDay', 'D_plus_1_pct', 'D_plus_5_pct'])
            # Write the data rows
            writer.writerows(data)
    except Exception as e:
        print(e)
        
def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Scrape a URL and download a file.')
    parser.add_argument('output_filename', help='The name of the output file (including extension)')

    args = parser.parse_args()

    # Create the download folder if it doesn't exist
    os.makedirs('temp', exist_ok=True)

    # Run the download function
    url = 'https://www.xoserve.com/help-centre/demand-attribution/unidentified-gas-uig/chart-uig-by-gas-day/'
    download_link_from_url(url, 'temp', args.output_filename)

if __name__ == '__main__':
    main()
