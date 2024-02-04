import os
import re
from datetime import datetime
import time

from cryptography.fernet import Fernet


# Retrieve user credentials in a "secure" fashion
def get_pg_credentials():
    TOKEN_FILE = os.environ['TOKEN_FILE']
    KEY_FILE = os.environ['KEY_FILE']

    with open(KEY_FILE, 'r') as f:
        key = re.search(r'^key=(.+)', f.read())[1]

    with open(TOKEN_FILE, 'r') as f:
        token = re.search(r'^token=(.+)', f.read())[1]

    cipher = Fernet(key)
    decoded_password = cipher.decrypt(token.encode('utf-8')).decode('utf-8')

    pg_creds = {
        'USER': os.environ['PGUSER'],
        'PASS': decoded_password
    }

    return pg_creds['USER'], pg_creds['PASS']


# decorator function for STDIO timestamps
def timestamp(func):
    def wrapper(*args, **kwargs):
        print(
            f'[{datetime.now()}]: executing query...',
            end='',
            flush=True
        )
        func(*args, **kwargs)
        print('complete.', flush=True)
        return func(*args, **kwargs)
    return wrapper


# decorator function for recording and displaying function run time
def get_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        print(f'complete: {end - start:.2f} seconds', flush=True)
    return wrapper
