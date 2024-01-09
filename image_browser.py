import os
import boto3
import pandas as pd
import streamlit as st
from PIL import Image
from io import BytesIO
import base64
from datetime import datetime
import pytz

s3 = boto3.client('s3', region_name=st.secrets['AWS_DEFAULT_REGION'], 
                  aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'], 
                  aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'])

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

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
    image.thumbnail((100, 100))

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
        st.title("🚽 Stool Image Browser")
        st.header("Storage")
        prefix = st.selectbox("Prefix", ["R0__ulcerative_colitis", "R1__bowel_preparation", "Calprotectin_Fecal_Test"])

    with st.spinner('Loading Data...'):
        df = get_s3_metadata(bucket, prefix)
    if df is not None:
        seoul_tz = pytz.timezone('Asia/Seoul')
        df['LastModified'] = df['LastModified'].dt.tz_convert(seoul_tz)
        df['LastModified'] = df['LastModified'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')

        with st.sidebar:
            st.header("Filter")
            col1, col2 = st.columns(2)

            with col1:
                start_date = pd.to_datetime(st.date_input('Start date', value=pd.to_datetime('today') - pd.DateOffset(days=7))).date()
            with col2:
                end_date = pd.to_datetime(st.date_input('End date', value=pd.to_datetime('today'))).date()

            st.header("Search")
            col1, col2 = st.columns(2)
            with col1:
                search_column = st.selectbox("Column", ["SiteName", "Gender", "DoB"])
            
            with col2:
                unique_values = ["-"] + sorted(df[search_column].unique().astype(str).tolist())
                search_query = st.selectbox("Value", unique_values)

            st.header("Sorting")
            col1, col2 = st.columns(2)
            with col1:
                sort_column = st.selectbox("Order by", ["LastModified", "DoB"])
            with col2:
                sort_order = st.selectbox("Strategy", ["Descending", "Ascending"])

        start_date = seoul_tz.localize(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0))
        end_date = seoul_tz.localize(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59))

        df_sel = df[(df['LastModified'] >= start_date.strftime('%Y-%m-%d %H:%M:%S %Z')) & (df['LastModified'] <= end_date.strftime('%Y-%m-%d %H:%M:%S %Z'))]

        # Apply search query
        if search_query != "-":
            df_sel = df_sel[df_sel[search_column].astype(str).str.contains(search_query, na=False)]

        # Apply sorting
        if sort_column:
            if sort_order == "Ascending":
                df_sel = df_sel.sort_values(by=sort_column, ascending=True)
            else:
                df_sel = df_sel.sort_values(by=sort_column, ascending=False)

        # Pagination
        items_per_page = 5
        n_pages = max(1, len(df_sel) // items_per_page)
        if len(df_sel) % items_per_page > 0:
            n_pages += 1

        if n_pages == 1 and len(df_sel) == 0:
            st.error('No data to display.')
        else:
            sup_col1, _, sup_col2 = st.columns([1.5, 2, 1.5])
            with sup_col2:
                st.subheader("💾 Download")
                all_rows = st.checkbox('All Rows?', value=True)

                # Export to CSV
                st.download_button(
                    label="Save as .csv",
                    data=convert_df(df if all_rows else df_sel),
                    file_name='stool_data_exported.csv',
                    mime='text/csv',
                )

            with sup_col1:
                st.subheader(f"Page ({st.session_state.page_number}/{n_pages})")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button('⬅️'):
                        if st.session_state.page_number > 1:
                            st.session_state.page_number -= 1
                            st.session_state.button_clicked = True
                with col2:
                    if st.button('➡️'):
                        if st.session_state.page_number < n_pages:
                            st.session_state.page_number += 1
                            st.session_state.button_clicked = True

            if st.session_state.button_clicked:
                st.session_state.button_clicked = False  # Reset the button click state
                st.rerun()  # Rerun the app

            start_index = items_per_page * (st.session_state.page_number - 1)
            end_index = start_index + items_per_page

            df_sel = df_sel.iloc[start_index:end_index]

        st.markdown("""<style>
                    .appview-container .main .block-container {
                        padding-top: 0rem;
                        width: 100%;
                        max-height: 80vh;
                    }
                    .st-emotion-cache-16txtl3 {
                        margin-top: -5rem;
                    }
                    #my_table {
                        width: 100%; 
                        margin-top: -1rem;
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
        
        st.markdown(df_sel.to_html(escape=False, index=False, table_id="my_table"), unsafe_allow_html=True)

    else:
        st.error('No objects found.')


if __name__ == '__main__':
    main()
