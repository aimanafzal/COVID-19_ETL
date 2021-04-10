# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import requests
from tqdm import tqdm
import sqlite3 as sqllite3
import csv as csv
import numpy as np
import pandas as pd

import os as os


def download_urls(URL):
    # Use a breakpoint in the code line below to debug your script.
    url_list = []
    response = requests.get(URL)
    for data in tqdm(response.json()):
        if data['name'].endswith('.csv'):
            url_list.append(data['download_url'])
    for i in range(len(url_list)):
        df = pd.read_csv(url_list[i])
        df.head()
        df.to_csv('out' + str(i) + '.csv')
    print('Done')
    return url_list


# Press the green button in the gutter to run the script.

relabel = {
    'Country/Region': 'Country_Region',
    'Lat': 'Latitude',
    'Long_': 'Longitude',
    'Province/State': 'Province_State'
}


def factor_dataframe(dat, filename):
    for label in dat:
        if label in relabel:
            dat = dat.rename(columns={label: relabel[label]})
    labels = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered']

    if 'Last_Update' not in dat:
        dat['Last_Update'] = pd.to_datetime(filename)

    for label in labels:
        if label not in dat:
            dat[label] = np.nan

    return dat[labels]


def upload_to_sql(filenames, db_name, debug=False):
    conn = sqllite3.connect(f"{db_name}.db")
    if debug:
        print("Uploading into database")
    for i, file_path in tqdm(list(enumerate(filenames))):
        dat = pd.read_csv(file_path)
        filename = os.path.basename(file_path).split('.')[0]
        dat = factor_dataframe(dat, filename)

        if i == 0:
            dat.to_sql(db_name, con=conn, index=False, if_exists='replace')
        else:
            dat.to_sql(db_name, con=conn, index=False, if_exists='append')


if __name__ == '__main__':
    # print_hi('PyCharm')
    URL = 'https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports'
    url_list = download_urls(URL)
    upload_to_sql(url_list, 'example', debug=True)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
