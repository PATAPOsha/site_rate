import csv
from pymongo import MongoClient, errors

head = ['url', 'name', 'category', 'info', 'region', 'bg_day', 'bg_week', 'bg_month',
        'iua_day', 'iua_week', 'iua_month']

with open('out.csv', 'ab') as f:
    writer = csv.writer(f)
    writer.writerow(head)

client = MongoClient('mongodb://localhost:27017/')
db = client['rate_db']
db_sites = db['sites']

items = db_sites.find({}, {'_id': 0})


with open('out.csv', 'ab') as f:
    writer = csv.writer(f)

    for item in items:
        try:
            reg = item['region']
        except:
            reg = '-'

        try:
            bg_day, bg_week, bg_month = item['bg_day'], item['bg_week'], item['bg_month']
        except:
            bg_day, bg_week, bg_month = '-', '-', '-'

        try:
            iua_day, iua_week, iua_month = item['iua_day'], item['iua_week'], item['iua_month']
        except:
            iua_day, iua_week, iua_month = '-', '-', '-'

        row = [item['url'], item['name'], item['category'], item['info'], reg]
        row = [x.encode('utf-8') for x in row]
        row += bg_day, bg_week, bg_month, iua_day, iua_week, iua_month

        writer.writerow(row)
