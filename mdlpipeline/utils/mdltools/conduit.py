import pandas as pd
import pysftp #0.2.8
from io import StringIO
import os

SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_USER = 'uws'
SFTP_PW = os.getenv('SFTP_PW')

def sftp_file_exists(sftp, filename):
    try:
        sftp.get(filename)
        return True
    except FileNotFoundError:
        return False

def put_conduit_file(string, fn):
    with pysftp.Connection(SFTP_HOST,
                           username=SFTP_USER,
                           password=SFTP_PW,
                           port=22222) as sftp:
        if not sftp_file_exists(sftp,'webcampus.uws.edu/conduit/' + fn):
            string_buf = StringIO(string)
            string_buf.seek(0)
            sftp.putfo(string_buf, 'webcampus.uws.edu/conduit/' + fn)

def transform_provision_df(obj, rename_map={}, action='create', **kwargs):
    # read data
    if isinstance(obj, str):
        pc_df = pd.read_excel(path, dtype=object)
    elif isinstance(obj, pd.DataFrame):
        pc_df = obj.copy()

    # Filter columns, rename columns, and drop empty rows
    if rename_map:
        pc_df = pc_df[list(rename_map.keys())].rename(columns=rename_map)
    else: 
         pc_df = pc_df[['idnumber']]

    # Drop empty rows
    pc_df.dropna(axis=0, how='all', inplace=True)

    # Create columns with default values
    pc_df['action'] = action
    
    # Add columns with default values
    for kwarg in kwargs:
        pc_df[kwarg] = kwargs[kwarg]

    # modify column data
    pc_df['idnumber'] = pc_df.idnumber.apply(lambda s: s.replace('P', '') if isinstance(s, str) else str(s))
    
    if 'email' in pc_df.columns:
        pc_df['username'] = pc_df.email.copy()

    return pc_df[['action'] + list(rename_map.values()) + list(kwargs.keys())]