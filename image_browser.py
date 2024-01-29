import re
import time
import pandas as pd
import streamlit as st
from datetime import datetime
import pytz
import zipfile

from utils import *

def main():
    bucket = st.secrets["AWS_S3_BUCKET_NAME"]

    with st.sidebar:
        st.title("🚽 Stool Image Browser")
        st.header("Storage")

        if 'guest' in st.session_state.username:
            folder_list = ['Calprotectin_Fecal_Test']
        else:
            folder_list = get_folder_list(bucket)

        default_prefix = st.session_state.prefix if st.session_state.prefix is not None else folder_list[0]
        prefix = st.selectbox("Prefix", folder_list, on_change=reset_session_state, index=folder_list.index(default_prefix))

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
        earliest_date = tz.localize(datetime(earliest_date.year, earliest_date.month, earliest_date.day, 0, 0, 0)).astimezone(tz)
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
                start_date = tz.localize(pd.to_datetime(st.date_input('Start date', value=earliest_date))).astimezone(tz).date()
            with col2:
                end_date = tz.localize(pd.to_datetime(st.date_input('End date', value=pd.to_datetime('today')))).astimezone(tz).date()

            start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
            end_date = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)

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
                st.header("Download")

                # download_link를 추출
                download_links = df_filter['Download'].apply(lambda x: re.search('href="(.+?)"', x).group(1) if re.search('href="(.+?)"', x) else None)

                df_to_download = df_filter[["SiteName", "Gender", "DoB", "LastModified", "Language"]].copy()
                df_to_download["FileName"] = [ f'image_{i:04d}' + get_image_ext(link) for i, link in enumerate(download_links.tolist(), start=1)]
                csv_data = convert_df(df_to_download)

                extract_button = st.empty()
                if extract_button.button(":arrow_upper_right: \r Extract"):
                    start_time = time.time()
                    progress_bar = st.progress(0, text='Extracting data...')
                    zip_generator = zip_files_parallel(download_links, csv_data)
                    for output in zip_generator:
                        if isinstance(output, float):
                            # 진행 상태 업데이트
                            progress = output

                            # ETA 계산 및 표시
                            elapsed_time = time.time() - start_time
                            eta = elapsed_time / progress * (1 - progress)
                            progress_bar.progress(output, text=f'Extracting data...(ETA: {eta:.2f} seconds)')
                        else:
                            # 최종 데이터 반환
                            zip_data = output
                            progress_bar.empty()
                            extract_button.empty()
                            st.success("🎉 Now, you can download data!")
                            st.download_button(label=':arrow_down: \r Download', data=zip_data, file_name='downloaded_files.zip', mime='application/zip')


            supcol1, _, supcol2 = st.columns([1, 5, 1])

            with supcol1:
                st.subheader(f"Page ({st.session_state.page_number}/{n_pages})")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button(':arrow_left: \r Prev') and st.session_state.page_number > 1:
                        st.session_state.page_number -= 1
                        st.session_state.button_clicked = True
                with col2:
                    if st.button('Next \r :arrow_right:') and st.session_state.page_number < n_pages:
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
