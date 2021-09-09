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

# passForm = st.form(key='pass')

# password = passForm.text_input('Enter password to enable content',
#                          type="password", help='Request access if needed')
# submit_button = passForm.form_submit_button(label='Submit')

# if password == st.secrets["appPass"] and submit_button:

# Streamlit App setup as a single state-based form

if 'validated' not in st.session_state:
    form = st.form(key='query')
    password = form.text_input('Enter password to enable content',
                               type="password", help='Request access if needed')
    login_button = form.form_submit_button(label='Login')

    if password == st.secrets["appPass"] and login_button:
        st.session_state.validated = True
    elif login_button:
        form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: ☠️ Incorrect Password</h3>",
                      unsafe_allow_html=True)

# State 1: Prompt File Uploaded
elif 'file' not in st.session_state:
    form = st.form(key='query')
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
        form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: {st.session_state.error}</h3>",
                      unsafe_allow_html=True)
    except:
        pass

# State 2: Ask user to select which merchant identifier is present in the uploaded file
elif 'id' not in st.session_state:

    form = st.form(key='query')
    form.markdown("<h3 style='text-align: center;'>Select from below the Merchant Identifier that was used in this list</h3>",
                  unsafe_allow_html=True)

    continue_button2 = form.form_submit_button(label='Continue')
    if continue_button2 and 'choice' in st.session_state:
        st.session_state.id = st.session_state.choice

    st.session_state.choice = st.radio(
        '', ['UID', 'MID', 'VID', 'Email'])


# State 3: Ask user to indicate which exact column in the uploaded file correlates with the selected merchant identifier
elif 'col' not in st.session_state:
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

    try:
        form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: {st.session_state.error}</h3>",
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

# State 4: Attempt loading the selected column from the file and if successful ask user to provide a unique name for the created list on HubSpot
elif 'upload' not in st.session_state:
    form = st.form(key='query')
    fileCheckPass = True
    df = pd.DataFrame()

    if st.session_state.file.name.lower().endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(
                st.session_state.file, sheet_name=st.session_state.sheet, engine='openpyxl', dtype='object')
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
        form.write(df.columns)
        form.write(st.session_state.col)
        st.session_state.error = '☠️ Column was NOT found in file'

    try:
        form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: {st.session_state.error}</h3>",
                      unsafe_allow_html=True)
    except:
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
    else:
        restart_button = form.form_submit_button(label='Start Over')
        if restart_button:
            for key in st.session_state.keys():
                del st.session_state[key]

# State 5: Attempt creating the list on HubSpot and adding the listed merchants to it
elif 'HSList' not in st.session_state:
    form = st.form(key='query')
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

    if st.session_state.id == 'Email':
        dfSQL[st.session_state.id] = dfSQL[st.session_state.id].astype(
            str)
        dfUpload[st.session_state.id] = dfUpload[st.session_state.id].astype(
            str)
    else:
        dfSQL[st.session_state.id] = dfSQL[st.session_state.id].astype(
            int)
        dfUpload[st.session_state.id] = dfUpload[st.session_state.id].astype(
            int)
    # try:
    st.session_state.result = pd.merge(
        dfSQL, dfUpload, on=st.session_state.id)

    # Create HubSpot List
    hubspotapi = st.secrets["hubspotapi"]
    writeURL = f"https://api.hubapi.com/contacts/v1/lists?hapikey={hubspotapi}"
    headers = {'Content-Type': "application/json"}
    body = {"name": st.session_state.list}
    payload = json.dumps(body)
    st.session_state.createListResp = requests.request(
        "POST", writeURL, data=payload, headers=headers)
    HSList = st.session_state.createListResp.json()['listId']
    st.session_state.HSList = HSList

    if 'error' in st.session_state:
        del st.session_state['error']

    if st.session_state.createListResp.status_code == 200:
        # Add merchants to created HubSpot List
        writeURL = f"https://api.hubapi.com/contacts/v1/lists/{st.session_state.HSList}/add?hapikey={hubspotapi}"
        body = {"emails": st.session_state.result['EMAIL'].tolist()}
        payload = json.dumps(body)
        st.session_state.addListResp = requests.request(
            "POST", writeURL, data=payload, headers=headers)
        if st.session_state.addListResp.status_code == 200:
            st.markdown(f"<h1 style='text-align: center;'> <a href='https://app.hubspot.com/contacts/6412394/lists/{st.session_state.HSList}'target='_blank'> Open list on HubSpot </a> </h1>",
                        unsafe_allow_html=True)
        else:
            st.session_state.error = '☠️ Failed to add merchants to list'
            st.markdown(
                f"<h3 style='text-align: center;color:Tomato'>ERROR: {st.session_state.error}</h3>", unsafe_allow_html=True)
            st.write(st.session_state.addListResp.json()['message'])
            restart_button2 = form.form_submit_button(label='Start Over')
            if restart_button2:
                for key in st.session_state.keys():
                    del st.session_state[key]
    else:
        st.session_state.error = '☠️ Failed to create list'
        st.markdown(
            f"<h3 style='text-align: center;color:Tomato'>ERROR: {st.session_state.error}</h3>", unsafe_allow_html=True)
        st.write(st.session_state.createListResp.json()['message'])
        restart_button3 = form.form_submit_button(label='Start Over')
        if restart_button3:
            for key in st.session_state.keys():
                del st.session_state[key]

    # except:
    #     st.session_state.error = '☠️ Unknown Error Occurred'
    #     form.markdown(f"<h3 style='text-align: center;color:Tomato'>ERROR: {st.session_state.error}</h3>",
    #                   unsafe_allow_html=True)
