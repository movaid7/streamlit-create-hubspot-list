#!/usr/bin/env python3
"""

Streamlit App to allow independent creation of Merchant Lists on HubSpot
App is setup as a single state-based form

"""

import json
import requests
import streamlit as st
import pandas as pd
import snowflake.connector

# =====================================================================================================================
# Config and Header
st.set_page_config(
    page_title="HubSpot List Creation Tool",
    page_icon="🔎"
    # layout="wide"
)

st.markdown(
    "![Alt Text](https://i.postimg.cc/DyJGmHZs/animation-500-kuzme348.gif)")

st.markdown("<h1 style='text-align: center;'>HubSpot List Creation Tool</h1>",
            unsafe_allow_html=True)
st.markdown("<h2 style='text-align: center;'>Create a list on HubSpot from a list of merchants</h2>",
            unsafe_allow_html=True)
st.markdown("***")


def error_screen():
    form = st.form(key='query')
    form.markdown(f"<h3 style='text-align: center;color:Tomato'>{st.session_state['error']}</h3>",
                  unsafe_allow_html=True)
    accept_button = form.form_submit_button(label='Accept')
    if accept_button:
        del st.session_state['error']


def login_screen():
    form = st.form(key='query')
    password = form.text_input('Enter password to enable content',
                               type="password", help='Request access if needed')
    login_button = form.form_submit_button(label='Login')

    if login_button and password == st.secrets["appPass"]:
        st.session_state.validated = True
    elif login_button:
        form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: ☠️ Incorrect Password. Please Try Again</h3>",
                      unsafe_allow_html=True)


def file_screen():
    form = st.form(key='query')
    form.markdown("<h3 style='text-align: center;'>Upload Excel/CSV List</h3>",
                  unsafe_allow_html=True)
    uploaded_file = form.file_uploader("")
    form.text(' ')
    form.text(' ')
    continue_button = form.form_submit_button(label='Continue')

    if continue_button and uploaded_file is not None:
        if uploaded_file.name.lower().endswith(('.xlsx', '.xls', 'csv')):
            st.session_state.file = uploaded_file
        else:
            st.session_state.error = '☠️ Unsupported File Type - .xlsx, .xls & .csv supported'
            return


def identifier_screen():
    form = st.form(key='query')
    form.markdown("<h3 style='text-align: center;'>Select from below the Merchant Identifier that was used in this list</h3>",
                  unsafe_allow_html=True)

    continue_button2 = form.form_submit_button(label='Continue')
    if continue_button2 and 'choice' in st.session_state:
        st.session_state.id = st.session_state.choice

    st.session_state.choice = st.radio(
        '', ['UID', 'MID', 'VID', 'EMAIL'])


def col_select_screen():
    form = st.form(key='query')
    sheetName = ""
    colName = ""
    if st.session_state.file.name.lower().endswith(('.xlsx', '.xls')):
        form.markdown("<h3 style='text-align: center;'>Enter the exact (case-sensitive) name of the SHEET IN YOUR EXCEL FILE that you wish to read data from</h3>",
                      unsafe_allow_html=True)
        sheetName = form.text_input(' ')
        form.text(' ')
        form.text(' ')

    form.markdown("<h3 style='text-align: center;'>Enter the exact (case-sensitive) name of the COLUMN that corresponds to the selected identifer</h3>",
                  unsafe_allow_html=True)
    colName = form.text_input('')
    form.text(' ')
    form.markdown("***")

    submit_button = form.form_submit_button(label='Submit')

    if submit_button:
        if sheetName != '' and colName != '':
            st.session_state.sheet = sheetName
            st.session_state.col = colName
        elif st.session_state.file.name.lower().endswith(('.csv')) and colName != '':
            st.session_state.col = colName
        else:
            form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: ☠️ Missing Input</h3>",
                          unsafe_allow_html=True)


def upload_screen():
    form = st.form(key='query')
    df = pd.DataFrame()
    form.text(f'File Name: {st.session_state.file.name}')
    form.text(' ')

    # Excel File
    if st.session_state.file.name.lower().endswith(('.xlsx', '.xls')):
        try:
            xl = pd.ExcelFile(st.session_state.file)
            df = xl.parse(st.session_state.sheet)
            form.text('✅ Sheet Found in File')
        except:
            del st.session_state['col']
            if 'col' in st.session_state:
                del st.session_state['sheet']
            st.session_state.error = '☠️ Sheet was NOT found in file'
            return

    # CSV File
    else:
        try:
            df = pd.read_csv(
                st.session_state.file, dtype='object')
        except:
            del st.session_state['col']
            if 'col' in st.session_state:
                del st.session_state['sheet']
            st.session_state.error = '☠️ Failed to open file'
            return
    # Check Column in file
    if st.session_state.col in list(df.columns):
        df = df[st.session_state.col].astype(str)
        df.rename(str(st.session_state.id), inplace=True)
        form.text('✅ Column Found in File')
        form.text(
            f'There are {len([x for x in df if x =="nan"])} null rows in the specified column')
        form.text(' ')
        form.subheader('Identfier Column Data')
        form.write(df)
    else:
        del st.session_state['col']
        if 'col' in st.session_state:
            del st.session_state['sheet']
        st.session_state.error = '☠️ Column was NOT found in file'
        return

    listName = ''
    form.markdown("<h3 style='text-align: center;'>Enter a UNIQUE name for your list</h3>",
                  unsafe_allow_html=True)
    listName = form.text_input('')
    form.text(' ')
    form.text(' ')
    create_button = form.form_submit_button(label='Create HubSpot List')
    if create_button and listName != '':
        st.session_state.upload = df
        st.session_state.list = listName
    elif create_button:
        form.markdown("<h3 style='text-align: center;color:Tomato'>ERROR: ☠️ Missing List Name</h3>",
                      unsafe_allow_html=True)


def success_screen():

    try:
        ctx = snowflake.connector.connect(
            user=st.secrets["user"],
            password=st.secrets["password"],
            account=st.secrets["account"],
            warehouse=st.secrets["warehouse"],
            database=st.secrets["database"],
            schema=st.secrets["schema"],
            ocsp_response_cache_filename="/tmp/ocsp_response_cache")

        sql = st.secrets["sql"]
        cur = ctx.cursor().execute(sql)
        dfSQL = cur.fetch_pandas_all()
        dfUpload = st.session_state.upload.to_frame()
    except:
        st.session_state.error = '☠️ Failed to Access Database'
        return

    try:
        # Drop nulls
        dfSQL.dropna(subset=[st.session_state.id], inplace=True)
        dfUpload = (dfUpload[~dfUpload.iloc[:, 0].str.contains("nan")])

        # Fix indices
        dfSQL.reset_index(drop=True)
        dfUpload.reset_index(drop=True)

        if st.session_state.id == 'EMAIL':
            dfSQL[st.session_state.id] = dfSQL[st.session_state.id].astype(
                str)
            dfUpload[st.session_state.id] = dfUpload[st.session_state.id].astype(
                str)
        else:
            dfSQL[st.session_state.id] = dfSQL[st.session_state.id].astype(
                int)
            dfUpload[st.session_state.id] = dfUpload[st.session_state.id].astype(
                int)

        st.session_state.result = pd.merge(
            dfSQL, dfUpload, on=st.session_state.id)

    except:
        st.session_state.error = '☠️ Dataset Issue'
        return

    # Create HubSpot List
    hubspotapi = st.secrets["hubspotapi"]
    writeURL = f"https://api.hubapi.com/contacts/v1/lists?hapikey={hubspotapi}"
    headers = {'Content-Type': "application/json"}
    body = {"name": st.session_state.list}
    payload = json.dumps(body)
    st.session_state.createListResp = requests.request(
        "POST", writeURL, data=payload, headers=headers)

    if st.session_state.createListResp.status_code == 200:
        HSList = st.session_state.createListResp.json()['listId']
        st.session_state.HSList = HSList
        # Add merchants to created HubSpot List
        writeURL = f"https://api.hubapi.com/contacts/v1/lists/{st.session_state.HSList}/add?hapikey={hubspotapi}"
        body = {"emails": st.session_state.result['EMAIL'].tolist()}
        payload = json.dumps(body)
        st.session_state.addListResp = requests.request(
            "POST", writeURL, data=payload, headers=headers)
        if st.session_state.addListResp.status_code == 200:
            st.markdown("<h1 style='text-align: center;color:Tomato'>ERROR: 🎉 Successfully Uploaded! </h1>",
                        unsafe_allow_html=True)
            st.markdown(f"<h2 style='text-align: center;'> <a href='https://app.hubspot.com/contacts/6412394/lists/{st.session_state.HSList}'target='_blank'> Open list on HubSpot </a> </h2",
                        unsafe_allow_html=True)
        else:
            st.session_state.error = f'☠️ Failed to add merchants to HubSpot list: {st.session_state.addListResp}'
            return
    else:
        st.session_state.error = f'☠️ Failed to create HubSpot list: {st.session_state.createListResp}'
        return


# Main App States
if 'error' in st.session_state:
    error_screen()

elif 'validated' not in st.session_state:
    login_screen()

elif 'file' not in st.session_state:
    file_screen()

elif 'id' not in st.session_state:
    identifier_screen()

elif 'col' not in st.session_state:
    col_select_screen()

elif 'upload' not in st.session_state:
    upload_screen()

else:
    success_screen()
