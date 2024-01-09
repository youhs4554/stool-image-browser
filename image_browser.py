import os
import boto3
import pandas as pd
import streamlit as st
from PIL import Image
from io import BytesIO
import base64
from datetime import datetime

s3 = boto3.client('s3', region_name=st.secrets['AWS_DEFAULT_REGION'], 
                  aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'], 
                  aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'])

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
            basename = os.path.basename(name)
            _, site_name, dob, gender, *_  = basename.split('_')

            data.append({'SiteName': site_name, 'Gender': gender, 'DoB' : datetime.strptime(dob, '%Y%m%d'), 'LastModified': last_modified, 'Preview': preview, 'Download': download_link})

    return pd.DataFrame(data)

@st.cache_resource
def get_s3_image_preview(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    image_data = obj['Body'].read()

    image = Image.open(BytesIO(image_data)).convert("RGB")
    image.thumbnail((150, 150))

    buffered = BytesIO()
    image.save(buffered, format="JPEG")

    img_str = base64.b64encode(buffered.getvalue()).decode()
    return f'<img src="data:image/jpeg;base64,{img_str}" />'

def get_s3_download_link(bucket, key):
    download_link = get_s3_presigned_url(bucket, key)
    return f'<a href="{download_link}" download>⬇</a>'


def main():
    bucket = st.secrets["AWS_S3_BUCKET_NAME"]

    with st.sidebar:
        st.title("AWS S3 prefix")
        prefix = st.selectbox("Prefix", ["R0__ulcerative_colitis", "R1__bowel_preparation", "Calprotectin_Fecal_Test"])
        st.title("Filter")
        col1, col2 = st.columns(2)

        with col1:
            start_date = pd.to_datetime(st.date_input('Start date', value=pd.to_datetime('today') - pd.DateOffset(days=7))).date()
        with col2:
            end_date = pd.to_datetime(st.date_input('End date', value=pd.to_datetime('today'))).date()

    st.info(f"Prefix : {prefix}")

    with st.spinner('Loading Data...'):
        df = get_s3_metadata(bucket, prefix)
    if df is not None:
        df['LastModified'] = df['LastModified'].dt.tz_localize(None)
        df['LastModified'] = df['LastModified'].dt.date

        with st.sidebar:
            col1, col2 = st.columns(2)
            with col1:
                search_column = st.selectbox("By", ["SiteName", "Gender"])
            
            with col2:
                unique_values = ["None"] + df[search_column].unique().tolist()
                search_query = st.selectbox("Select value", unique_values)

            st.title("Sorting")
            col1, col2 = st.columns(2)
            with col1:
                sort_column = st.selectbox("Order by", ["DoB", "LastModified"])
            with col2:
                sort_order = st.selectbox("Strategy", ["Ascending", "Descending"])


        df = df[(df['LastModified'] >= start_date) & (df['LastModified'] <= end_date)]

        # Apply search query
        if search_query != "None":
            df = df[df[search_column].astype(str).str.contains(search_query, na=False)]

        # Apply sorting
        if sort_column:
            if sort_order == "Ascending":
                df = df.sort_values(by=sort_column, ascending=True)
            else:
                df = df.sort_values(by=sort_column, ascending=False)
        
        # Pagination
        items_per_page = 10
        n_pages = max(1, len(df) // items_per_page)
        if len(df) % items_per_page > 0:
            n_pages += 1

        if n_pages == 1 and len(df) == 0:
            st.error('No data to display.')
        else:
            with st.sidebar:
                st.title("Page Slider")

                page_number = st.slider(label="page_number", min_value=1, max_value=n_pages, value=1, step=1)

                # Export to Excel
                st.title("Download")
                towrite = BytesIO()
                df[["SiteName", "DoB", "Gender"]].to_excel(towrite, index=False, engine='openpyxl')  
                towrite.seek(0)
                b64 = base64.b64encode(towrite.read()).decode()
                linko= f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="mytable.xlsx">Export all data as an excel file</a>'
                st.markdown(linko, unsafe_allow_html=True)

            start_index = items_per_page * (page_number - 1)
            end_index = start_index + items_per_page

            df = df.iloc[start_index:end_index]

        st.markdown("""<style>
                    .main > div {
                        padding: 1rem;
                        height: 80vh;
                    }
                    .table-container {
                        max-height: 80vh;
                        overflow-y: auto;
                    }
                    #my_table {
                        width: 100%; 
                        table-layout: auto;
                    }
                    #my_table th {
                        background-color: #f8f9fa; 
                        color: #333; 
                        font-size: 1.5em; 
                        text-align: center;
                        font-style: italic;
                    }
                    #my_table td {
                        font-size: 1em; 
                        text-align: center;
                    }
                    </style>
                    """, unsafe_allow_html=True)
        
        st.markdown(f'<div class="table-container">{df.to_html(escape=False, index=False, table_id="my_table")}</div>', unsafe_allow_html=True)

    else:
        st.error('No objects found.')


if __name__ == '__main__':
    main()
