#!/usr/bin/env python3
import datetime as dt
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
# from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]


def convert_tz(dt_object):
    tz_time = dt.datetime.strptime(dt_object, "%Y-%m-%dT%H:%M:%S.%f%z")
    return tz_time.replace(tzinfo=dt.timezone.utc).astimezone(tz=None)


def main():

    PATH_TO_CREDS = '/home/rickey/.google_creds/'
    TOKEN = os.path.join(PATH_TO_CREDS, 'token.json')
    CREDENTIALS = os.path.join(PATH_TO_CREDS, 'credentials.json')

    creds = None

    if os.path.exists(TOKEN):
        creds = Credentials.from_authorized_user_file(TOKEN, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS, SCOPES)

            creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(TOKEN, "w") as token:
                token.write(creds.to_json())

    try:
        # create drive api client
        service = build("drive", "v3", credentials=creds)
        files = []
        page_token = None

        response = (
            service.files().list(
                        q="name='dq_creeper_output.csv'",
                        spaces="drive",
                        fields="nextPageToken, files(name, modifiedTime)",
                        pageToken=page_token).execute()
            )

        for file in response.get("files", []):
            convert_tz(file.get("modifiedTime"))
            print(
                    f'\n{file.get("name")} last updated: '
                    f'{convert_tz(file.get("modifiedTime"))}\n'
            )

            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

    except HttpError as error:
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    main()
