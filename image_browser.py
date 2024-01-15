import os
import re
import boto3
import pandas as pd
import streamlit as st
from PIL import Image
from io import BytesIO
import base64
from datetime import datetime
import pytz
import requests
import zipfile
import concurrent.futures

s3 = boto3.client('s3', region_name=st.secrets['AWS_DEFAULT_REGION'], 
                  aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'], 
                  aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'])
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

# zip 파일로 압축
@st.cache_data
def zip_files(download_links, csv_data):
    zip_buffer = BytesIO()

    def download_and_write(item):
        i, link = item
        data = download_data(link)
        ext = get_image_ext(link)
        with zipfile.ZipFile(zip_buffer, 'a') as zip_file:  # 'a' mode to append
            zip_file.writestr(os.path.join('images', f'image_{i:04d}' + ext), data)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        list(executor.map(download_and_write, enumerate(download_links, start=1)))

    with zipfile.ZipFile(zip_buffer, 'a') as zip_file:
        zip_file.writestr('meta_table.csv', csv_data)

    return zip_buffer.getvalue()

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

def get_s3_metadata(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if response.get('Contents') is None:
        return None

    data = []
    for obj in response['Contents']:
        name = obj['Key']
        last_modified = obj['LastModified']
        if is_image_file(name):
            preview = get_s3_image_preview(bucket, name)
            download_link = get_s3_download_link(bucket, name)
            basename = os.path.splitext(os.path.basename(name))[0]
            _, site_name, dob, gender, _, _, *lang  = basename.split('_')

            if lang:
                lang = lang[0]
            else:
                lang = "N/A"

            data.append({'SiteName': site_name, 'Gender': gender, 'DoB' : datetime.strptime(dob, '%Y%m%d'), 'LastModified': last_modified, 'Language': lang, 'Preview': preview, 'Download': download_link})

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


def main(username):
    bucket = st.secrets["AWS_S3_BUCKET_NAME"]

    with st.sidebar:
        st.title("🚽 Stool Image Browser")
        st.header("Storage")

        if 'guest' in username:
            folder_list = ['Calprotectin_Fecal_Test']
        else:
            folder_list = get_folder_list(bucket)

        prefix = st.selectbox("Prefix", folder_list)

        # 사용자에게 선택할 수 있는 시간대 리스트를 제공합니다.
        us_timezones = ['America/New_York', 'America/Denver', 'America/Chicago', 'America/Los_Angeles']
        timezones = ['Asia/Seoul', *us_timezones]
        st.header("Time Zone")
        # Streamlit의 selectbox를 사용하여 사용자에게 시간대를 선택하도록 요청합니다.
        selected_timezone = st.selectbox('Please select your timezone', timezones)

    with st.spinner('Loading Data...'):
        df = get_s3_metadata(bucket, prefix)
    if df is not None:
        # 선택한 시간대를 적용합니다.
        tz = pytz.timezone(selected_timezone)

        # 기존 코드를 선택한 시간대에 맞게 수정합니다.
        earliest_date = pd.to_datetime(df['LastModified']).min()
        earliest_date = tz.localize(datetime(earliest_date.year, earliest_date.month, earliest_date.day, 0, 0, 0))
        df['LastModified'] = df['LastModified'].dt.tz_convert(tz)
        df['LastModified'] = df['LastModified'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')

        with st.sidebar:
            st.header("Sorting")
            col1, col2 = st.columns(2)
            with col1:
                sort_column = st.selectbox("Order by", ["LastModified", "DoB"])
            with col2:
                sort_order = st.selectbox("Strategy", ["Descending", "Ascending"])
            
            # Apply sorting
            if sort_column:
                if sort_order == "Ascending":
                    df = df.sort_values(by=sort_column, ascending=True)
                else:
                    df = df.sort_values(by=sort_column, ascending=False)

        with st.sidebar:
            st.header("Filter")
            col1, col2 = st.columns(2)
            with col1:
                start_date = pd.to_datetime(st.date_input('Start date', value=earliest_date)).date()
            with col2:
                end_date = pd.to_datetime(st.date_input('End date', value=pd.to_datetime('today'))).date()

            start_date = tz.localize(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0))
            end_date = tz.localize(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59))

            df_sel = df[(df['LastModified'] >= start_date.strftime('%Y-%m-%d %H:%M:%S %Z')) & (df['LastModified'] <= end_date.strftime('%Y-%m-%d %H:%M:%S %Z'))]

            df_filter = df_sel

            def reset_page_number():
                st.session_state.page_number = 1
            
            col1, col2, col3 = st.columns(3)
            with col1:
                site_name_options = ["-"] + sorted(df_filter["SiteName"].unique().astype(str).tolist())
                site_name = st.selectbox("SiteName", site_name_options, on_change=reset_page_number)
                if site_name != "-":
                    df_filter = df_filter[df_filter["SiteName"].astype(str).str.contains(site_name, na=False)]
            with col2:
                gender_options = ["-"] + sorted(df_filter["Gender"].unique().astype(str).tolist())
                gender = st.selectbox("Gender", gender_options, on_change=reset_page_number)
                if gender != "-":
                    df_filter = df_filter[df_filter["Gender"].astype(str).str.contains(gender, na=False)]
            with col3:
                dob_options =  ["-"] + sorted(df_filter["DoB"].unique().astype(str).tolist())
                dob = st.selectbox("DoB", dob_options, on_change=reset_page_number)
                if dob != "-":
                    df_filter = df_filter[df_filter["DoB"].astype(str).str.contains(dob, na=False)]
            
            st.header("Rows per page")
            items_per_page = st.slider('Rows', 5, 50, value=5, step=5)

        # re-index
        df_filter.index = pd.Series(range(1, len(df_filter)+1))
        df_filter.reset_index(inplace=True)

        # Pagination
        n_pages = max(0, len(df_filter) // items_per_page)
        if len(df_filter) % items_per_page > 0:
            n_pages += 1

        if n_pages == 1 and len(df_filter) == 0:
            st.error('No data to display.')
        else:
            st.info(f"📊 Number of samples : {len(df_filter)}")
            with st.sidebar:
                st.header("Download Data")

                # download_link를 추출
                download_links = df_filter['Download'].apply(lambda x: re.search('href="(.+?)"', x).group(1) if re.search('href="(.+?)"', x) else None)

                df_to_download = df_filter[["SiteName", "Gender", "DoB", "LastModified", "Language"]].copy()
                df_to_download["FileName"] = [ f'image_{i:04d}' + get_image_ext(link) for i, link in enumerate(download_links.tolist(), start=1)]
                csv_data = convert_df(df_to_download)

                # zip 파일 생성
                zip_data = zip_files(download_links, csv_data)

                # download_button을 사용하여 zip 파일 다운로드
                st.download_button(label='⬇️', data=zip_data, file_name='downloaded_files.zip', mime='application/zip')

            supcol1, _, supcol2 = st.columns([1, 8, 1])

            with supcol1:
                st.subheader(f"Page ({st.session_state.page_number}/{n_pages})")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button('⬅️') and st.session_state.page_number > 1:
                        st.session_state.page_number -= 1
                        st.session_state.button_clicked = True
                with col2:
                    if st.button('➡️') and st.session_state.page_number < n_pages:
                        st.session_state.page_number += 1
                        st.session_state.button_clicked = True
            
            start_index = items_per_page * (st.session_state.page_number - 1)
            end_index = start_index + items_per_page

            df_filter = df_filter.iloc[start_index:end_index]
        
        st.markdown("""<style>
                    .appview-container .main .block-container {
                        padding-top: 0rem;
                        width: 100%;
                        max-height: 80vh;
                    }
                    .st-emotion-cache-16txtl3 {
                        margin-top: -5rem;
                    }
                    .table-container {
                        max-height: 80vh;
                        overflow-y: auto;
                    }
                    #my_table {
                        width: 100%; 
                    }
                    #my_table th {
                        background-color: #f8f9fa; 
                        color: #333; 
                        font-size: 1.2em; 
                        text-align: center;
                        font-style: italic;
                    }
                    #my_table td {
                        text-align: center;
                    }
                    </style>
                    """, unsafe_allow_html=True)
        
        st.markdown(f'<div class="table-container">{df_filter.to_html(escape=False, index=False, table_id="my_table")}</div>', unsafe_allow_html=True)

    else:
        st.error('No objects found.')

    if st.session_state.button_clicked:
        st.session_state.button_clicked = False  # Reset the button click state
        st.rerun()  # Rerun the app

if __name__ == '__main__':
    main()
