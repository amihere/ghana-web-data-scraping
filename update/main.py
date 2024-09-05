import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import types
from ulid import ULID
import sqlite3

# Helpers
#


# function to generate ulid
def generate_ulid(row):
    row["id"] = int(ULID())
    return row


# getting the urls to the different sectiions
def get_section_urls(row):
    df = pd.DataFrame()
    if row['section'] in section_list:
        temp_row = pd.DataFrame(data=[[row["section"], row["url"]]],
                                columns=df_columns)
        df = pd.concat([df, temp_row], ignore_index=True)
    else:
        pass
    return df


# load env
BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
load_dotenv(os.path.join(BASE_DIR, '.env'))

# retrieving data from Sitemap
base_url = os.getenv("BASE_URL")
request_url = f"{base_url}/GhanaHomePage/sitemap.php"
page = requests.get(request_url)

soup = BeautifulSoup(page.content, 'html.parser')
results = soup.find(id="medsection2")
site_map_data = results.find_all("dl", class_="resources")

df_columns = ['section', 'url']
sections_df = pd.DataFrame(columns=df_columns)

for sections in site_map_data:
    urls = sections.find_all("a")
    for url in urls:
        temp_row = pd.DataFrame(data=[[url.text, base_url + url.get('href')]],
                                columns=df_columns)
        sections_df = pd.concat([sections_df, temp_row], ignore_index=True)

# Selecting Story Sections with articles
section_list = ["Abroad, Ghanaians", "African News", "Athletics",
                "BBC Hausa News", "BBC Pidgin News", "Business & Economy",
                "Coronavirus", "Crime News", "Editorial News", "Entertainment",
                "Health News", "Lifestyle", "Music", "Political News",
                "Regional News", "Sports Section", "Tabloid News"]

df = sections_df.apply(lambda row: get_section_urls(row), axis=1)

df_columns = ['section', 'url']
stories_urls_df = pd.DataFrame(columns=df_columns)

for row in df:
    stories_urls_df = pd.concat([stories_urls_df, row])

stories_urls_df.reset_index(inplace=True, drop=True)

stories_urls_df = stories_urls_df.apply(lambda row: generate_ulid(row), axis=1)

stories_urls_df["created"] = datetime.now()

stories_urls_df = stories_urls_df[["id", "section", "url", "created"]]

# push data to db
# create engine
SQL_DATABASE = os.getenv("SQL_DATABASE")

# Connect to the db
with sqlite3.connect(SQL_DATABASE) as conn:
    # create df schema
    df_schema = dict(zip(stories_urls_df.columns.tolist(),
                         (types.BIGINT,
                          types.VARCHAR(length=150),
                          types.VARCHAR(length=1000),
                          types.TIMESTAMP)))
    # store data
    stories_urls_df.to_sql("sections", con=conn,
                           dtype=df_schema, index=False)
