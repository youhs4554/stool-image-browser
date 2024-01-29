import numpy as np
import streamlit as st
from app import login
from image_browser import convert_df, get_folder_list, get_s3_metadata
from utils import reset_session_state
from datetime import datetime
import pytz
import pandas as pd
import plotly.express as px

def set_streamlit_page_config_once():
    try:
        st.set_page_config(page_title="Stool Data Statistics", page_icon="📊", layout='wide')
    except st.errors.StreamlitAPIException as e:
        if "can only be called once per app" in e.__str__():
            # ignore this error
            return
        raise e
    
set_streamlit_page_config_once()

if 'authenticator' not in st.session_state:
    st.session_state.authenticator = None
if 'prefix' not in st.session_state:
    st.session_state.prefix = None

def statistics_page():    
    st.markdown("""
                <style>
                    .appview-container .main .block-container {
                        padding-top: 0rem;
                        width: 100%;
                        max-height: 80vh;
                    }
                    .instruction {
                        background-color: rgba(152, 251, 152, 0.5); 
                        font-size: 1.5rem;
                        padding: 0.5rem;
                    }
                    </style>
                    """, unsafe_allow_html=True)
    
    bucket = st.secrets["AWS_S3_BUCKET_NAME"]
    with st.sidebar:
        st.title("📊 Stool Data Statistics")
        st.header("Storage")

        if 'guest' in st.session_state['username']:
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

        st.header("Duration")
        col1, col2 = st.columns(2)
        with col1:
            start_date = pd.to_datetime(st.date_input('Start date', value=pd.to_datetime('today')-pd.Timedelta(days=7))).date()
        with col2:
            end_date = pd.to_datetime(st.date_input('End date', value=pd.to_datetime('today'))).date()

    if st.session_state.prefix != prefix:
        st.session_state.prefix = prefix
        st.rerun()

    st.markdown(f'<div class="instruction">Full data is available <a href="/" onclick="window.history.back(); return false;">here</a></div>', unsafe_allow_html=True)
    st.title(f"Report ({start_date.strftime('%Y/%m/%d')} - {end_date.strftime('%Y/%m/%d')})")
    st.text(f"Name : {st.session_state.prefix.strip('/')}")
    with st.spinner('Loading Data...'):
        df = get_s3_metadata(bucket, prefix, fetch_preview=False)
    if df is not None:
        # 선택한 시간대를 적용합니다.
        tz = pytz.timezone(selected_timezone)

        df.rename({'LastModified': 'UploadDate'}, axis=1, inplace=True)

        # 시간대에 맞게 수정합니다.
        df['UploadDate'] = df['UploadDate'].dt.tz_convert(tz)
        df['UploadDate'] = df['UploadDate'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        df = df.sort_values(by="UploadDate", ascending=False)

        start_date = tz.localize(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)).astimezone(tz)
        end_date = tz.localize(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)).astimezone(tz)

        df_sel = df[(df['UploadDate'] >= start_date.strftime('%Y-%m-%d %H:%M:%S %Z')) & (df['UploadDate'] <= end_date.strftime('%Y-%m-%d %H:%M:%S %Z'))]
        if len(df_sel) == 0:
            st.error('No data to display.')
        else:
            # Normalize data
            df_sel.index = pd.Series(range(1, len(df_sel)+1))
            df_new = df_sel.copy().drop(['Download', 'Language'], axis=1).reset_index()
            df_new['Gender'] = df_new['Gender'].replace({'Male': '남자', 'Female': '여자'})
            df_new['SiteName'] = df_new['SiteName'].replace({'KNUH': '경북대병원'})
            df_new['DoB'] = df_new['DoB'].dt.strftime('%Y-%m-%d')

            # 성별과 생년월일로 환자를 특정 가능하다고 가정 -> 중복되는 행 제외 (환자 카운팅 목적)
            df_new_unique = df_new.drop_duplicates(['Gender', 'DoB'])

            tz_str = df_new['UploadDate'].iloc[0].split(' ')[-1]
            df_new['UploadDate'] = pd.to_datetime(df_new['UploadDate'], format='%Y-%m-%d %H:%M:%S ' + tz_str)

            st.subheader('Data Preview')
            st.text(f"N_samples = {len(df_new)}, N_patients = {len(df_new_unique)}")
            st.text('(Only 10 rows are displayed and sorted by upload time)')
            show_all = st.checkbox('Show all rows', False)
            n_rows = len(df_new) if show_all else 10
            st.text('For more data...')
            st.download_button(label=':arrow_down: \r Download as .csv', data=convert_df(df_new), file_name='report.csv')
            st.table(df_new[['SiteName', 'Gender', 'DoB', 'UploadDate']].sort_values('UploadDate', ascending=False).head(n_rows))

            st.subheader("Statistics")
            column_name = st.selectbox("Select column", ["All", "Gender", "SiteName", "DoB", "Upload Date"])
            def replace_numeric_with_etc(value):
                # 만약 값이 숫자라면 'etc'를 반환하고, 그렇지 않으면 원래의 값을 반환합니다.
                return 'etc' if value=="" else value
            
            df_new['SiteName'] = df_new['SiteName'].apply(replace_numeric_with_etc)
            if column_name == 'Gender' or column_name == 'All':
                gender_counts = df_new_unique['Gender'].value_counts()  # 각 성별의 수를 계산합니다.
                fig1 = px.pie(df_new_unique, names=gender_counts.index, values=gender_counts.values, title='Pie Chart of Gender')
                st.plotly_chart(fig1)
            if column_name == 'SiteName' or column_name == 'All':
                site_counts = df_new_unique['SiteName'].value_counts()  # 각 병원의 수를 계산합니다.
                fig2 = px.pie(df_new_unique, names=site_counts.index, values=site_counts.values, title='Pie Chart of SiteName') # plotly pie차트
                st.plotly_chart(fig2)
            if column_name == 'DoB' or column_name == 'All':
                fig3 = px.histogram(df_new_unique, x='DoB', nbins=100, title='DoB Histogram')
                st.plotly_chart(fig3)
            if column_name == 'Upload Date' or column_name == 'All':
                # 'UploadDate' 컬럼의 날짜와 시간 부분만 추출합니다.
                df_new['DateTime'] = df_new['UploadDate'].dt.floor('H')

                # 각 시간에 발생한 이벤트 횟수를 계산합니다.
                event_counts = df_new['DateTime'].value_counts().sort_index()

                # 전체 시간 범위를 생성합니다.
                full_dates = pd.date_range(start=event_counts.index.min(), end=event_counts.index.max(), freq='H')

                # 생성한 시간 범위를 데이터프레임으로 변환합니다.
                df_dates = pd.DataFrame(full_dates, columns=['DateTime'])

                # 이벤트가 발생한 횟수를 표시하는 새로운 컬럼을 생성합니다.
                df_dates['Upload Count'] = df_dates['DateTime'].map(event_counts).fillna(0)

                # Scatter plot을 그립니다.
                fig4 = px.line(df_dates, x='DateTime', y='Upload Count', title='Upload Event Distribution per Hour', markers=True)

                st.plotly_chart(fig4)



if __name__ == "__main__":
    name, authentication_status, username, authenticator = login()
    if authentication_status:
        statistics_page()
        with st.sidebar:
            st.header("Logout?")
            authenticator.logout('Logout', 'main', key='unique_key')
    elif authentication_status == False:
        st.error('Username/password is incorrect')
    elif authentication_status == None:
        st.warning("Only authorized users can access this database.")
