import streamlit as st

def reset_session_state():
    st.session_state.page_number = 1
    st.session_state.button_clicked = False
    st.session_state.apply_filter = False
