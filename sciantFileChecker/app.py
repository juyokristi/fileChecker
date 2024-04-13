import streamlit as st
import boto3
import xml.etree.ElementTree as ET
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_xml_data(access_key, secret_key, region, bucket_name, directory):
    logging.debug("Starting to fetch XML data from AWS S3")
    try:
        # Initialize a session with AWS using provided credentials and region
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        s3_client = session.client('s3')
        logging.info(f"Connected to S3 bucket: {bucket_name} in region: {region}")

        # List objects in the specified S3 bucket and directory
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
        files_info = []

        for item in response.get('Contents', []):
            file_key = item['Key']
            if file_key.endswith('.xml') and 'history_forecast' not in file_key:  # Filter out unwanted files
                logging.debug(f"Processing file: {file_key}")
                obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                xml_content = obj['Body'].read()
                root = ET.fromstring(xml_content)

                # Extract necessary data from XML
                business_date = root.find('.//BUSINESS_DATE').text if root.find('.//BUSINESS_DATE') is not None else 'N/A'
                generation_time = root.find('.//GENERATION_TIME').text if root.find('.//GENERATION_TIME') is not None else 'N/A'
                from_date = root.find('.//FROM_DATE').text if root.find('.//FROM_DATE') is not None else 'N/A'
                to_date = root.find('.//TO_DATE').text if root.find('.//TO_DATE') is not None else 'N/A'

                files_info.append({
                    'File Name': file_key.split('/')[-1],
                    'Business Date': business_date,
                    'Generation Time': generation_time,
                    'From Date': from_date,
                    'To Date': to_date
                })

        logging.info("Data fetching completed successfully")
        return pd.DataFrame(files_info)
    except Exception as e:
        logging.error("Failed to fetch data from S3", exc_info=True)
        return pd.DataFrame()

def main():
    st.title('XML File Data Extractor from AWS S3')
    st.subheader('Please enter your AWS configuration:')

    with st.form(key='my_form'):
        access_key = st.text_input('AWS Access Key ID')
        secret_key = st.text_input('AWS Secret Access Key', type='password')
        region = st.text_input('AWS Region')
        bucket_name = st.text_input('S3 Bucket Name')
        directory = st.text_input('Directory Path')
        submit_button = st.form_submit_button(label='Fetch Data')

    if submit_button:
        if access_key and secret_key and region and bucket_name and directory:
            df = fetch_xml_data(access_key, secret_key, region, bucket_name, directory)
            if df.empty:
                st.warning("No data found. Check the settings or the content of the bucket.")
            else:
                st.dataframe(df)  # Display the DataFrame in an interactive table
        else:
            st.error("Please fill in all the fields.")

if __name__ == '__main__':
    main()
