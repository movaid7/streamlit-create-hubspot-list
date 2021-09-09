import math
import requests
import json
import streamlit as st
import pandas as pd
import snowflake
import snowflake.connector
import boto3

# Config and Header
st.set_page_config(
    page_title="HubSpot List Creation Tool",
    page_icon="🔎"
    # layout="wide"
)
st.markdown("<h1 style='text-align: center;'>HubSpot List Creation Tool</h1>",
            unsafe_allow_html=True)
st.markdown("<h2 style='text-align: center;'>Create a list on HubSpot from a list of merchants</h2>",
            unsafe_allow_html=True)
st.markdown("***")


form = st.form(key='query')

if 'file' not in st.session_state:
    form.markdown("<h3 style='text-align: center;'>Upload Excel/CSV List</h3>",
                  unsafe_allow_html=True)
    uploaded_file = form.file_uploader("")
    form.text(' ')
    form.text(' ')
    continue_button = form.form_submit_button(label='Continue')

    if continue_button and uploaded_file is not None:
        if 'error' in st.session_state:
            del st.session_state['error']
        if uploaded_file.name.lower().endswith(('.xlsx', '.xls', 'csv')):
            st.session_state.file = uploaded_file
        else:
            st.session_state.error = '☠️ Unsupported File Type (.xlsx, .xls & .csv supported)'
    try:
        form.markdown(f"<h3 style='text-align: center;color:red'>ERROR: {st.session_state.error}</h3>",
                      unsafe_allow_html=True)
    except:
        pass

elif 'id' not in st.session_state:
    st.markdown(f"<h4 style='text-align: center;'>File Uploaded: '{st.session_state.file.name}'</h3>",
                unsafe_allow_html=True)
    form.markdown("<h3 style='text-align: center;'>Select the Merchant Identifier that was used in this list</h3>",
                  unsafe_allow_html=True)
    searchOption = form.radio(
        '', ['UID', 'MID', 'VID', 'Email'])
    form.text(' ')
    form.text(' ')

    continue_button = form.form_submit_button(label='Continue')

    if continue_button and searchOption is not None:
        st.session_state.id = searchOption

elif 'col' not in st.session_state:
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

    try:
        form.markdown(f"<h3 style='text-align: center;color:red'>ERROR: {st.session_state.error}</h3>",
                      unsafe_allow_html=True)
    except:
        pass

    submit_button = form.form_submit_button(label='Submit')

    if submit_button:
        if 'error' in st.session_state:
            del st.session_state['error']
        if sheetName != '' and colName != '':
            st.session_state.sheet = sheetName
            st.session_state.col = colName
        elif st.session_state.file.name.lower().endswith(('.csv')) and colName != '':
            st.session_state.col = colName
        else:
            st.session_state.error = 'Missing Input!'

elif 'upload' not in st.session_state:
    fileCheckPass = True
    df = pd.DataFrame()

    if st.session_state.file.name.lower().endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(
                st.session_state.file, sheet_name=st.session_state.sheet, dtype='object')
            form.text('✅ Sheet Found in File')
        except:
            st.session_state.error = '☠️ Sheet was NOT found in file'

    else:
        try:
            df = pd.read_csv(
                st.session_state.file, dtype='object')
        except:
            st.session_state.error = '☠️ Failed to open file'

    if st.session_state.col in list(df.columns):
        df = df[st.session_state.col].astype(str)
        df.rename(str(st.session_state.id), inplace=True)
        form.text('✅ Column Found in File')
        form.text(
            f'There are {len([x for x in df if x =="nan"])} null rows for selected column')
        form.subheader('Identfier Column Data')
        form.write(df)
    else:
        st.session_state.error = '☠️ Column was NOT found in file'

    try:
        form.markdown(f"<h3 style='text-align: center;color:red'>ERROR: {st.session_state.error}</h3>",
                      unsafe_allow_html=True)
    except:
        listName = ''
        form.markdown("<h3 style='text-align: center;'>Enter a name for your list</h3>",
                      unsafe_allow_html=True)
        listName = form.text_input('')
        form.text(' ')
        form.text(' ')
        create_button = form.form_submit_button(label='Create HubSpot List')
        if create_button and listName != '':
            st.session_state.upload = df
            st.session_state.list = listName
    else:
        restart_button = form.form_submit_button(label='Start Over')
        if restart_button:
            for key in st.session_state.keys():
                del st.session_state[key]

elif 'HSList' not in st.session_state:
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
    # dfRet = cur.fetch_pandas_batches()
    dfSQL = cur.fetch_pandas_all()
    dfUpload = st.session_state.upload.to_frame()

    # st.write(dfUpload.head())
    # st.write(dfSQL.head())
    if st.session_state.id == 'Email':
        dfSQL[st.session_state.id] = dfSQL[st.session_state.id].astype(str)
        dfUpload[st.session_state.id] = dfUpload[st.session_state.id].astype(
            str)
    else:
        dfSQL[st.session_state.id] = dfSQL[st.session_state.id].astype(int)
        dfUpload[st.session_state.id] = dfUpload[st.session_state.id].astype(
            int)

    # try:
    st.session_state.result = pd.merge(
        dfSQL, dfUpload, on=st.session_state.id)
    # st.write(st.session_state.result.head())

    # Create HubSpot List
    hubspotapi = st.secrets["hubspotapi"]
    writeURL = f"https://api.hubapi.com/contacts/v1/lists?hapikey={hubspotapi}"
    headers = {'Content-Type': "application/json"}
    body = {"name": st.session_state.list}
    payload = json.dumps(body)
    # POST to HubSpot
    st.session_state.createListResp = requests.request(
        "POST", writeURL, data=payload, headers=headers)
    HSList = st.session_state.createListResp.json()['listId']
    st.session_state.HSList = HSList

    if 'error' in st.session_state:
        del st.session_state['error']

    if st.session_state.createListResp.status_code == 200:
        writeURL = f"https://api.hubapi.com/contacts/v1/lists/{st.session_state.HSList}/add?hapikey={hubspotapi}"
        body = {"emails": st.session_state.result['EMAIL'].tolist()}
        payload = json.dumps(body)
        st.session_state.addListResp = requests.request(
            "POST", writeURL, data=payload, headers=headers)
        if st.session_state.addListResp.status_code == 200:
            # st.markdown(f"<h3 style='text-align: center;'>href='https://app.hubspot.com/contacts/6412394/lists/{st.session_state.HSList}'Open list on HubSpot</h3>",
            #             unsafe_allow_html=True)
            st.markdown(f"<h1 style='text-align: center;'> <a href='https://app.hubspot.com/contacts/6412394/lists/{st.session_state.HSList}'target='_blank'> Open list on HubSpot </a> </h1>",
                        unsafe_allow_html=True)
        else:
            st.session_state.error = '☠️ Failed to add merchants to list'
            st.markdown(
                f"<h3 style='text-align: center;color:red'>ERROR: {st.session_state.error}</h3>", unsafe_allow_html=True)
            st.write(st.session_state.addListResp.json()['message'])
            restart_button = form.form_submit_button(label='Start Over')
            if restart_button:
                for key in st.session_state.keys():
                    del st.session_state[key]
    else:
        st.session_state.error = '☠️ Failed to create list'
        st.markdown(
            f"<h3 style='text-align: center;color:red'>ERROR: {st.session_state.error}</h3>", unsafe_allow_html=True)
        st.write(st.session_state.createListResp.json()['message'])
        restart_button = form.form_submit_button(label='Start Over')
        if restart_button:
            for key in st.session_state.keys():
                del st.session_state[key]

    # except:
    #     st.session_state.error = '☠️ Unknown Error Occurred'
    #     form.markdown(f"<h3 style='text-align: center;color:red'>ERROR: {st.session_state.error}</h3>",
    #                   unsafe_allow_html=True)
