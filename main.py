# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import requests
from tqdm import tqdm

import csv as csv
import numpy as np
import pandas as pd
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
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    URL = 'https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports'
    download_urls(URL)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
