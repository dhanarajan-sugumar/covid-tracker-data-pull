import decimal
import json
import random
import string
import datetime
import urllib
import boto3
import pandas as pd
from boto3.dynamodb.types import DYNAMODB_CONTEXT

# Monkey patch Decimal's default Context to allow
# inexact and rounded representation of floats
# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

"""Setting up dynamodb client for localhost"""
# dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8001")
# dynamodb = boto3.client('dynamodb', endpoint_url='http://localhost:8001')

dynamodb = boto3.client('dynamodb', region_name='us-east-1')


def random_string(string_length=8):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.sample(letters, string_length))


def delete_table(table_name):
    response = dynamodb.list_tables()

    if table_name in response['TableNames']:
        dynamodb.delete_table(TableName=table_name)
        print('Waiting for', table_name, '...')
        waiter_for_delete = dynamodb.get_waiter('table_not_exists')
        waiter_for_delete.wait(TableName=table_name)
        print(">> Deleted Table")
    else:
        print(">> Table NOT Found")


def create_table(table_name):
    print('creating table')
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'index',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'country',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'index',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'country',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        },
        StreamSpecification={
            'StreamEnabled': False
        },
    )
    # Wait for the table to exist before exiting
    print('Waiting for', table_name, '...')
    waiter = dynamodb.get_waiter('table_exists')
    waiter.wait(TableName=table_name)
    print("create table succeed")


def main(event=None, context=None):
    current_date = datetime.date.today()
    Previous_Date = datetime.datetime.today()-datetime.timedelta(days=1)
    table_name = "tcovid19"

    delete_table(table_name)
    create_table(table_name)

    try:
        covid19_source_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data" \
                             "/csse_covid_19_daily_reports/" + current_date.strftime("%m-%d-%Y") + ".csv"
        csv = pd.read_csv(covid19_source_url, usecols=["Province_State", "Country_Region", "Last_Update",
                                                       "Confirmed", "Deaths", "Recovered"])
        print(covid19_source_url)

    except urllib.error.HTTPError:
        print(Previous_Date)
        covid19_source_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data" \
                             "/csse_covid_19_daily_reports/" + Previous_Date.strftime("%m-%d-%Y") + ".csv"
        print(">> Pulling previous day data >> " + covid19_source_url)
        csv = pd.read_csv(covid19_source_url, usecols=["Province_State", "Country_Region", "Last_Update",
                                                       "Confirmed", "Deaths", "Recovered"])

    jsonstr = csv.to_json(path_or_buf=None, orient="records", date_format='epoch', double_precision=10,
                          force_ascii=True, date_unit='ms', default_handler=None)
    data = json.loads(jsonstr, parse_float=decimal.Decimal)
    for eachdata in data:
        country = eachdata['Country_Region']
        if eachdata['Province_State']:
            state = eachdata['Province_State']
        else:
            state = "Not Available"
        last_update = eachdata['Last_Update']
        confirmed = eachdata['Confirmed']
        deaths = eachdata['Deaths']
        recovered = eachdata['Recovered']

        # print("<< PUT COVID-19 data:", country, state, last_update, confirmed, deaths, recovered)

        dynamodb.put_item(TableName=table_name,
                          Item={
                              'index': {'S': random_string(8)},
                              'country': {'S': country},
                              'state': {'S': state},
                              'lastupdate': {'S': last_update},
                              'confirmed': {'N': str(confirmed)},
                              'deaths': {'N': str(deaths)},
                              'recovered': {'N': str(recovered)}
                          })
    print(">> PUT COVID-19 data:")


if __name__ == "__main__":
    main()
