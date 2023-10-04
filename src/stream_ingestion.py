"""Application to ingest streaming data from online website and calculate LTV"""

import json
import operator
from datetime import datetime, timedelta


def ingest(e: dict, D: list) -> list:
    """function to ingest each event and store it in a standard format
    with keys and details separated out
    e: event
    D: list of events"""
    record = {'details': {}}
    for key in e:
        if key in ['type', 'verb', 'key', 'event_time', 'customer_id']:
            record[key] = e[key]
        else:
            record['details'][key] = e[key]
    if record['type'] == 'CUSTOMER':
        record['customer_id'] = record['key']
    D.append(record)
    return D


def TopXSimpleLTVCustomers(x: int, D: list) -> list:
    """Function to calculate the LTV value for each customer
    and return x number of top customers
    x: Number of customer with top LTV
    D: list of events"""

    # Find the weeks between oldest and newest Order event
    max_ts = max([rec['event_time'] for rec in D if rec['type'] == 'ORDER'])

    min_ts = min([rec['event_time'] for rec in D if rec['type'] == 'ORDER'])

    format_ts = '%Y-%m-%dT%H:%M:%S.%f%z'
    d1 = datetime.strptime(min_ts, format_ts)
    d2 = datetime.strptime(max_ts, format_ts)
    m1 = (d1 - timedelta(days=d1.weekday()))
    m2 = (d2 - timedelta(days=d2.weekday()))
    wk = int((m2 - m1).days / 7) + 1

    # Sort the reformatted event data on customer ID and event_time
    D_sorted = sorted(D, key=operator.itemgetter('customer_id', 'key', 'event_time'))

    # Retain only the latest event for each key
    D_latest = list({rec['key']: rec for rec in D_sorted}.values())

    t_amts = {}
    t_sites = {}
    cust_ids = []
    # Find all customer IDs, total amounts of orders and total site visits by customer IDs
    for rec in D_latest:
        if rec['type'] == 'ORDER':
            t_amts[rec['customer_id']] = t_amts.get(rec['customer_id'], 0) + \
                                         float(str(rec['details']['total_amount']).
                                               replace('USD', '').replace(' ', ''))
        if rec['type'] == 'SITE_VISIT':
            t_sites[rec['customer_id']] = t_sites.get(rec['customer_id'], 0) + 1
        if rec['type'] == 'CUSTOMER':
            cust_ids.append(rec['key'])

    # Calculate the LTV for each customer
    t_ltvs = []
    for cust_id in cust_ids:
        print(cust_id,' ',t_amts.get(cust_id, 0),' ', wk,' ', t_sites.get(cust_id, 0))
        t_ltv = {'customer_id': cust_id,
                 'ltv': round(((t_amts.get(cust_id, 0) / wk) *
                         (t_sites.get(cust_id, 0) / wk)) * 10, 2)}
        t_ltvs.append(t_ltv)
    return sorted(t_ltvs, key=operator.itemgetter('ltv'), reverse=True)[:x]


def main():
    with open('..\\input\\input.txt', 'r') as file:
        data = file.read()
    messages = json.loads(data)

    D = []
    # Reformat each event into a simplified structure of key fields and details
    for event in messages:
        ingest(event, D)

    with open("..\\output\\output.txt", 'w') as file:
        file.write(json.dumps(TopXSimpleLTVCustomers(2, D)))


if __name__ == "__main__":
    main()
