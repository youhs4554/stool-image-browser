import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
import os
import zipfile
import pandas as pd
import requests
import streamlit as st
import boto3
from PIL import Image

s3 = boto3.client('s3', region_name=st.secrets['AWS_DEFAULT_REGION'], 
                  aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'], 
                  aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'])

def reset_session_state():
    st.session_state.page_number = 1
    st.session_state.button_clicked = False
    st.session_state.apply_filter = False

@st.cache_data
def get_image_ext(url):
    name = os.path.basename(url).split("?")[0]
    _, ext = os.path.splitext(name)
    return ext

# download_link에서 데이터를 다운로드 받아 byte string으로 변환
@st.cache_data
def download_data(url):
    response = requests.get(url)
    return response.content


def download_and_compress(i, link, zip_file, total_files):
    data = download_data(link)
    ext = get_image_ext(link)
    zip_file.writestr(os.path.join('images', f'image_{i:04d}' + ext), data)
    progress = (i+1) / total_files
    return progress

# zip 파일로 압축 (병렬 처리 : on)
def zip_files_parallel(download_links, csv_data):
    total_files = len(download_links)
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        with ThreadPoolExecutor() as executor:
            for i, link in enumerate(download_links):
                progress = executor.submit(download_and_compress, i, link, zip_file, total_files)
                yield progress.result()

        zip_file.writestr('meta_table.csv', csv_data)
    yield zip_buffer.getvalue()

def get_folder_list(bucket):
    folder_list = []
    paginator = s3.get_paginator('list_objects_v2')
    for result in paginator.paginate(Bucket=bucket, Delimiter='/'):
        for prefix in result.get('CommonPrefixes'):
            folder_list.append(prefix.get('Prefix'))

    return folder_list

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False)

@st.cache_data
def get_s3_presigned_url(bucket, key, expiration=3600):
    try:
        response = s3.generate_presigned_url('get_object',
                                             Params={'Bucket': bucket, 'Key': key},
                                             ExpiresIn=expiration)
    except Exception as e:
        print(e)
        return None

    return response

def is_image_file(file_name):
    image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff']
    extension = os.path.splitext(file_name)[1]
    if extension.lower() in image_extensions:
        return True
    else:
        return False

def get_s3_metadata(bucket, prefix, fetch_preview=True):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if response.get('Contents') is None:
        return None

    data = []
    for obj in response['Contents']:
        name = obj['Key']
        last_modified = obj['LastModified']
        if is_image_file(name):
            if fetch_preview:
                preview = get_s3_image_preview(bucket, name)
            else:
                preview = None
            download_link = get_s3_download_link(bucket, name)
            basename = os.path.splitext(os.path.basename(name))[0]
            _, site_name, dob, gender, _, _, *lang  = basename.split('_')

            if lang:
                lang = lang[0]
            else:
                lang = "N/A"

            row = {'SiteName': site_name, 'Gender': gender, 'DoB' : datetime.strptime(dob, '%Y%m%d'), 'LastModified': last_modified, 'Language': lang, 'Preview': preview, 'Download': download_link}
            if not fetch_preview:
                del row['Preview']
            data.append(row)
    
    return pd.DataFrame(data)

@st.cache_resource
def get_s3_image_preview(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    image_data = obj['Body'].read()

    image = Image.open(BytesIO(image_data)).convert("RGB")
    image.thumbnail((100, 100))

    buffered = BytesIO()
    image.save(buffered, format="JPEG")

    img_str = base64.b64encode(buffered.getvalue()).decode()
    return f'<img src="data:image/jpeg;base64,{img_str}" />'

def get_s3_download_link(bucket, key):
    download_link = get_s3_presigned_url(bucket, key)
    name = os.path.basename(download_link.split('?')[0])
    name, ext = os.path.splitext(name)
    img_prefix, site_name, dob, gender, date_info, time_info, *lang = name.split('_')
    outfile_elements = [img_prefix]

    if site_name != "":
        outfile_elements.append(site_name)

    outfile_elements.extend([dob, gender, date_info, time_info])

    if lang:
        outfile_elements.extend(lang)

    outfile = "_".join(outfile_elements) + ext

    return f'<a href="{download_link}" download="{outfile}">⬇</a>'
