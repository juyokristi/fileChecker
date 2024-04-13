import streamlit as st
import boto3
import xml.etree.ElementTree as ET
import pandas as pd

def fetch_xml_data(access_key, secret_key, region, bucket_name, directory):
    # Initialize a session with AWS using provided credentials and region
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3_client = session.client('s3')
    files_info = []

    # List objects in the specified S3 bucket and directory
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
    for item in response.get('Contents', []):
        file_key = item['Key']
        if file_key.endswith('.xml'):  # Check if the file is an XML file
            # Get the object from S3 and read its content
            obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            xml_content = obj['Body'].read()
            root = ET.fromstring(xml_content)

            # Extract necessary data from XML
            business_date = root.find('.//BUSINESS_DATE').text if root.find('.//BUSINESS_DATE') is not None else 'N/A'
            generation_time = root.find('.//GENERATION_TIME').text if root.find('.//GENERATION_TIME') is not None else 'N/A'
            from_date = root.find('.//FROM_DATE').text if root.find('.//FROM_DATE') is not None else 'N/A'
            to_date = root.find('.//TO_DATE').text if root.find('.//TO_DATE') is not None else 'N/A'

            # Append data to the list as a dictionary
            files_info.append({
                'File Name': file_key.split('/')[-1],
                'Business Date': business_date,
                'Generation Time': generation_time,
                'From Date': from_date,
                'To Date': to_date
            })

    # Return data as a pandas DataFrame
    return pd.DataFrame(files_info)

def main():
    st.title('XML File Data Extractor from AWS S3')
    st.subheader('Please enter your AWS configuration:')

    # Form for user input
    with st.form(key='my_form'):
        access_key = st.text_input('AWS Access Key ID')
        secret_key = st.text_input('AWS Secret Access Key', type='password')
        region = st.text_input('AWS Region')
        bucket_name = st.text_input('S3 Bucket Name')
        directory = st.text_input('Directory Path')
        submit_button = st.form_submit_button(label='Fetch Data')

    # Process user input
    if submit_button:
        if access_key and secret_key and region and bucket_name and directory:
            df = fetch_xml_data(access_key, secret_key, region, bucket_name, directory)
            st.dataframe(df)  # Display the DataFrame in an interactive table
        else:
            st.error("Please fill in all the fields.")  # Show error if any field is missing

if __name__ == '__main__':
    main()
