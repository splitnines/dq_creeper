#!/usr/bin/env python3

import asyncio
import selectors
import json
import re
import time
import os
import datetime as dt
from collections import deque
import traceback
import pandas as pd
from requests.structures import CaseInsensitiveDict
from aiohttp import ClientSession
from sqlalchemy.types import VARCHAR, DATE, TEXT, INT
from cryptography.fernet import Fernet

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


async def http_get(url, session):
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://practiscore.com/',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99",'
        ' "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/92.0.4515.131 Safari/537.36',
        'X-CSRF-TOKEN': '2ml0QNDDNyYOr9MtxKRdXGV9WGeGh68xtnf3hcBH'
    }

    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.text()


async def http_post(url, headers, data, session):
    async with session.post(url, headers=headers, data=data) as response:
        if response.status == 200:
            return await response.json()


async def http_sess(match_uuid_list):
    def_tasks = deque()

    async with ClientSession() as session:
        for match_uuid in match_uuid_list:
            url1 = (
                'https://s3.amazonaws.com/ps-scores/'
                f"production/{match_uuid}/match_def.json"
            )
            def_tasks.append(asyncio.create_task(http_get(url1, session)))
        return (x for x in await asyncio.gather(*def_tasks))


async def http_sess2(search_list):
    def_tasks = deque()

    async with ClientSession() as session:
        for search_str in search_list:
            url, headers, data = ps_search_config(search_str[0], 1000)
            def_tasks.append(
                asyncio.create_task(http_post(url, headers, data, session))
            )

        return (x for x in await asyncio.gather(*def_tasks))


def event_loop(func, *args):
    selector = selectors.SelectSelector()
    asyncio.new_event_loop()
    loop = asyncio.SelectorEventLoop(selector)
    asyncio.set_event_loop(loop)
    response = (loop.run_until_complete(func(*args)))
    loop.close()
    return response


def ps_search_config(club_name_search, num_hits_str):
    url = (
        'https://1x6b6xdr0h-dsn.algolia.net/1/indexes/*/'
        'queries?x-algolia-agent=Algolia%20for%20vanilla%'
        '20JavaScript%20(lite)%203.30.0%3Binstantsearch.js'
        '%202.10.5%3BJS%20Helper%202.26.1&x-algolia-'
        'application-id=1X6B6XDR0H&x-algolia-api-key='
        'eb57a76c0fa721c3fb3a908d0cf5746d'
    )

    headers = CaseInsensitiveDict()
    headers["Connection"] = "keep-alive"
    headers["sec-ch-ua"] = (
        '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"'
    )
    headers["accept"] = "application/json"
    headers["content-type"] = "application/x-www-form-urlencoded"
    headers["sec-ch-ua-mobile"] = "?0"
    headers["User-Agent"] = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/97.0.4692.71 Safari/537.36'
    )
    headers["sec-ch-ua-platform"] = 'Windows'
    headers["Origin"] = "https://practiscore.com"
    headers["Sec-Fetch-Site"] = "cross-site"
    headers["Sec-Fetch-Mode"] = "cors"
    headers["Sec-Fetch-Dest"] = "empty"
    headers["Referer"] = "https://practiscore.com/"
    headers["Accept-Language"] = "en-US,en;q=0.9"

    data = (
        '{"requests":[{"indexName":"postmatches","params":'
        f'"query={club_name_search}&hitsPerPage={num_hits_str}'
        '&maxValuesPerFacet=10&page=0&facets='
        '%5B%22templateName%22%5D&tagFilters='
        '&facetFilters=%5B%5B%22templateName%3AUSPSA%22%5D%5D"},'
        '{"indexName":'
        f'"postmatches","params":"query={club_name_search}'
        f'&hitsPerPage={num_hits_str}&maxValuesPerFacet=10&page='
        '0&attributesToRetrieve=%5B%5D&attributesToHighlight='
        '%5B%5D&attributesToSnippet=%5B%5D&tagFilters=&analytics='
        'false&clickAnalytics=false&facets=templateName"}]}'
    )

    return url, headers, data


def get_aws_files(data_dict):
    results_list = []

    for result in data_dict.values():
        match_uuid_list = []

        for match in result[1]['results'][0]['hits']:
            if re.search(result[0], match['match_name']):
                match_uuid_list.append(match['match_id'])

        try:
            match_def_data = event_loop(http_sess, match_uuid_list)
        except Exception:
            raise Exception(traceback.format_exc())

        match_def_json = (
            (json.loads(i) for i in match_def_data if type(i) is str)
        )
        results_list.append(match_def_json)
    return results_list


def get_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        print(f'complete: {end - start:.2f} seconds', flush=True)
    return wrapper


@get_time
def google_drive_copy(filename, scopes) -> None:

    TOKEN = os.environ['GOOGLETOKEN']
    CREDENTIALS = os.environ['GOOGLECREDS']

    creds = None

    if os.path.exists(TOKEN):
        creds = Credentials.from_authorized_user_file(TOKEN, scopes)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS, scopes
            )
            creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(TOKEN, "w") as token:
                token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)

        results = service.files().list(
                q=f'name="{filename}"and mimeType="text/csv"',
                spaces='drive'
            ).execute()

        if results['files'][0]['name'] == filename:
            media = MediaFileUpload(
              results['files'][0]['name'],
              mimetype=results['files'][0]['mimeType']
            )
            service.files().update(
                fileId=results['files'][0]['id'], media_body=media
            ).execute()

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"[{dt.datetime.now()}]: An error occurred: {error}")


# retreive postgres credentials
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


# pull database table
def get_db_table(conn):

    db_df = pd.read_sql_table('dqcreeper', conn, schema='dqcreeper')

    db_df.drop(['id'], axis=1, inplace=True)

    db_df.sort_values(
        by=['date', 'match'],
        ascending=False,
        ignore_index=True,
        inplace=True
    )

    return db_df


# compare dataframes and keep only new entries
def get_new_dq_entries(df1, df2):

    df1 = df1.merge(
        df2, indicator=True, how='outer'
    ).query('''_merge == "left_only"''').drop('_merge', axis=1)

    return df1


# write new entries to database
def write_to_db(df, conn) -> None:
    if len(df.index) > 0:
        sql_dtypes = {
            'date': DATE,
            'match': TEXT,
            'lastname': VARCHAR(length=50),
            'firstname': VARCHAR(length=50),
            'uspsanum': VARCHAR(length=15),
            'id': INT,
        }

        df.to_sql(
            'dqcreeper',
            conn,
            schema='dqcreeper',
            if_exists='append',
            dtype=sql_dtypes,
            index=False,
        )


@get_time
def main() -> None:

    # start = time.time()

    HOMEDIR = os.path.expanduser('~')
    OUTPUT_FILENAME = 'dq_creeper_output.csv'
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    CLUB_NAME_SEARCH_LIST = [
        ('lcsc', re.compile('lcsc', re.IGNORECASE)),
        ('richmond hot', re.compile('richmond hot', re.IGNORECASE)),
        ('ncps', re.compile('ncps', re.IGNORECASE)),
        ('capsl', re.compile('capsl', re.IGNORECASE)),
        ('targetmasters', re.compile('target *masters?', re.IGNORECASE)),
        ('soap', re.compile('soap', re.IGNORECASE)),
        ('lrc', re.compile('lrc', re.IGNORECASE)),
        ('shasta', re.compile('shasta', re.IGNORECASE)),
        ('edps', re.compile('edps', re.IGNORECASE)),
        ('hosemonster', re.compile('hose *monsters?', re.IGNORECASE)),
        ('ccpl', re.compile('ccpl', re.IGNORECASE)),
        ('5 dogs', re.compile('5 *dogs', re.IGNORECASE)),
        ('slosa', re.compile('slosa', re.IGNORECASE)),
        ('rankin field', re.compile('rankin *field', re.IGNORECASE)),
        ('tehama', re.compile('tehama', re.IGNORECASE)),
        ('dap', re.compile('dap', re.IGNORECASE)),
        ('golden bullet', re.compile('golden bullet', re.IGNORECASE)),
        ('roadrunner', re.compile('roadrunner', re.IGNORECASE)),
        ('gridiron', re.compile('gridiron', re.IGNORECASE)),
        ('coastal classic', re.compile('coastal classic', re.IGNORECASE)),
        ('deseert classic', re.compile('desert classic', re.IGNORECASE)),
    ]

    ps_query_time = time.time()
    print(
        f'\n[{dt.datetime.now()}]: executing Practiscore queries...',
        end='',
        flush=True
    )

    try:
        results_data = event_loop(http_sess2, CLUB_NAME_SEARCH_LIST)
    except Exception:
        raise Exception(traceback.format_exc())
    print(f'complete: {time.time() - ps_query_time:.2f} seconds', flush=True)

    data_dict = {}
    for club, data in zip(CLUB_NAME_SEARCH_LIST, results_data):
        data_dict[club[0]] = (club[1], data)

    aws_data_pull_time = time.time()
    print(
        f'[{dt.datetime.now()}]: executing AWS data pulls....',
        end='',
        flush=True
    )
    results_list = get_aws_files(data_dict)
    print(
        f'complete: {time.time() - aws_data_pull_time:.2f} seconds', flush=True
    )

    data_comp_time = time.time()
    print(f'[{dt.datetime.now()}]: compiling data....', end='', flush=True)
    df = pd.DataFrame(
        columns=['Date', 'Match', 'Last Name', 'First Name', 'USPSA#']
    )

    for match in results_list:
        for match_def in match:
            for shooter in match_def['match_shooters']:
                if 'sh_dq' in shooter and shooter['sh_dq'] is True:

                    match_date = match_def['match_date']
                    match_name = (
                        match_def['match_name'].replace(',', '').strip()
                    )
                    shooter_ln = shooter['sh_ln'].upper()
                    shooter_fn = shooter['sh_fn'].upper()

                    if 'sh_id' in shooter and shooter['sh_id'] != '':

                        shooter_id = re.sub(
                            r'[^a-zA-Z0-9]', '', shooter['sh_id']
                        ).upper()

                        dq_row = [
                            match_date, match_name, shooter_ln,
                            shooter_fn, shooter_id
                        ]

                    else:

                        shooter_id = 'NO SHOOTER ID'

                        dq_row = [
                            match_date, match_name, shooter_ln,
                            shooter_fn, shooter_id
                        ]

                    df.loc[len(df)] = dq_row

    df.drop_duplicates(inplace=True)
    df.sort_values(
        by=['Date', 'Match'],
        inplace=True,
        ascending=False,
        ignore_index=True
    )

    # HOMEDIR = os.path.expanduser('~')
    csv_fn = os.path.join(
        HOMEDIR, 'dq_creeper', OUTPUT_FILENAME
    )
    df.to_csv(csv_fn, index=False)

    print(f'complete: {time.time() - data_comp_time:.2f} seconds')

    db_read_time = time.time()
    print(f'[{dt.datetime.now()}]: reading db table....', end='')
    user, passwd = get_pg_credentials()
    conn = f'postgresql://{user}:{passwd}@10.0.0.203/postgres'
    db_df = get_db_table(conn)

    print(f'complete: {time.time() - db_read_time:.2f} seconds')

    col_rename = {
        'Date': 'date',
        'Match': 'match',
        'Last Name': 'lastname',
        'First Name': 'firstname',
        'USPSA#': 'uspsanum',
    }
    df.rename(columns=col_rename, inplace=True)
    df = df.astype({'date': 'datetime64[ns]'})
    df.sort_values(
        by=['date', 'match'],
        ascending=False,
        ignore_index=True,
        inplace=True
    )

    df = get_new_dq_entries(df, db_df)

    db_write_time = time.time()
    print(
        f'[{dt.datetime.now()}]: writing to db table....',
        end='',
        flush=True
    )
    write_to_db(df, conn)
    print(f'complete: {time.time() - db_write_time:.2f} seconds', flush=True)

    print(f'[{dt.datetime.now()}]: report saved to: {csv_fn}', flush=True)

    print(
        f'[{dt.datetime.now()}]: copying '
        f'{OUTPUT_FILENAME} to Google Drive.....', end='', flush=True
    )
    google_drive_copy(OUTPUT_FILENAME, SCOPES)

    print(f'[{dt.datetime.now()}]: ', end='', flush=True)


if __name__ == "__main__":
    main()
