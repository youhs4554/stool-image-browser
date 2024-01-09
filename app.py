import streamlit as st
import streamlit_authenticator as stauth
from image_browser import main as image_browser

st.set_page_config(page_title="Stool Image Browser", page_icon="🚽", layout='wide')

def login():
    global authenticator
    st.title("Stool Image Browser")

    authenticator = stauth.Authenticate(
        st.secrets["credentials"].to_dict(),
        st.secrets['cookie']['name'],
        st.secrets['cookie']['key'],
        st.secrets['cookie']['expiry_days'],
        st.secrets['preauthorized']
    )

    placeholder_text = st.empty()
    placeholder_text.text("Only privileged users can access this database.")

    name, authentication_status, username = authenticator.login('Login', 'main')
    
    if authentication_status:
        placeholder_text.empty()

    return name, authentication_status, username

if __name__ == "__main__":
    name, authentication_status, username = login()

    if authentication_status:
        st.toast(f'Login Success. Welcome {name}!')
        image_browser()
    elif authentication_status == False:
        st.error('Username/password is incorrect')
    elif authentication_status == None:
        st.warning('Please enter your username and password')
