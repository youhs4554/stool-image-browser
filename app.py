import streamlit as st
import streamlit_authenticator as stauth
from image_browser import main as image_browser


def login():
    st.set_page_config(page_title="Stool Image Browser", page_icon="🚽", layout='wide')
    global authenticator

    placeholder_title = st.empty()
    placeholder_title.title("🚽 Stool Image Browser")

    authenticator = stauth.Authenticate(
        st.secrets["credentials"].to_dict(),
        st.secrets['cookie']['name'],
        st.secrets['cookie']['key'],
        st.secrets['cookie']['expiry_days'],
        st.secrets['preauthorized']
    )

    name, authentication_status, username = authenticator.login('Login', 'main')
    
    if authentication_status:
        placeholder_title.empty()

    return name, authentication_status, username

if __name__ == "__main__":
    if 'page_number' not in st.session_state:
        st.session_state.page_number = 1
    if 'button_clicked' not in st.session_state:
        st.session_state.button_clicked = False
    if 'apply_filter' not in st.session_state:
        st.session_state.apply_filter = False

    name, authentication_status, username = login()

    if authentication_status:
        image_browser()
        with st.sidebar:
            st.header("Logout?")
            authenticator.logout('Logout', 'main', key='unique_key')
    elif authentication_status == False:
        st.error('Username/password is incorrect')
    elif authentication_status == None:
        st.warning("Only authorized users can access this database.")
