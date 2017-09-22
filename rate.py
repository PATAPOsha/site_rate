# -*- coding: utf-8 -*-
import requests
import time
from bs4 import BeautifulSoup
import urllib
from multiprocessing import freeze_support
from multiprocessing.pool import ThreadPool, Pool
import json
from pymongo import MongoClient, errors
import re


get_head = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'ru-RU, en-EN, uk-EN, en-us',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
}


def bg_parse_group(arr):
    url, gr_name = arr
    out = []
    for i in range(10):
        r = requests.get(url + str(i) + '/', headers=get_head, verify=False)
        s = BeautifulSoup(r.content, 'lxml')
        tds = s.find_all('td', class_='lpad10px llink')
        if tds:
            for x in tds:
                href = x.find('a')['href']
                try:
                    site_url = urllib.unquote(href.split('url=')[1]).replace('https:', 'http:')
                except:
                    site_url = href.replace('https:', 'http:')
                if site_url[-1] == '/':
                    site_url = site_url[:-1]

                # print site_url
                info_div = x.find('div', class_='small gray')
                bg_url = 'http://top.bigmir.net/report/%s/' % info_div['id']
                info = info_div.text.strip()
                out.append([bg_url, site_url, info, gr_name])
        else:
            break
    return out


def bg_parse_item(arr):
    try:
        url, site, info, gr_name = arr
        r = requests.get(url, headers=get_head, verify=False)
        s = BeautifulSoup(r.content, 'lxml')

        name = s.find('h4').find('a').text.strip()
        tds = [x.text.strip().replace(' ', '') for x in
               s.find('table', class_='nowrap small text_right').find_all('tr')[1].find_all('td')]

        try:
            day = int(tds[2])
        except ValueError:
            day = None
        try:
            week = int(tds[4])
        except ValueError:
            week = None
        try:
            month = int(tds[5])
        except ValueError:
            month = None
        # print day, week, month
        try:
            site = site.encode('utf-8')
        except:
            site = site.decode('cp1251').encode('utf-8')

        site = site.replace('.www', '')
        return {'url': site, 'name': name, 'category': gr_name, 'info': info,
                'bg_day': day, 'bg_week': week, 'bg_month': month}
    except Exception as e:
        print url
        print e
        print r.text


def grab_bg():
    r = requests.get('http://top.bigmir.net/main/', headers=get_head, verify=False)
    s = BeautifulSoup(r.content, 'lxml')
    groups = [['http://top.bigmir.net' + x['href'], x.text.strip()] for x in s.find_all('a', class_='large')]
    st = time.time()

    pool = Pool(10)
    urls = pool.map(bg_parse_group, groups)
    pool.close()
    pool.join()

    urls = [item for sublist in urls for item in sublist]

    pool = Pool(2)
    sites = pool.map(bg_parse_item, urls)
    pool.close()
    pool.join()

    sites = [x for x in sites if x]
    # print json.dumps(sites[-1])
    client = MongoClient('mongodb://localhost:27017/')
    db = client['rate_db']
    db_sites = db['sites']
    try:
        if 'url' not in db_sites.index_information():
            db_sites.create_index('url', unique=True)
    except errors.OperationFailure:
        db_sites.create_index('url', unique=True)

    for x in sites:
        db_sites.insert_one(x)

    print time.time() - st


# ------------------------------------- i.ua


def parse_item_iua(url):
    try:
        r = requests.get(url, headers=get_head, verify=False)
        s = BeautifulSoup(r.text, 'lxml')

        site_tag = s.find('p', class_='green marginB02')
        info = site_tag.find_next().text.strip().encode('utf-8')
        site = 'http://' + site_tag.text.strip().encode('utf-8').replace('.www', '')
        name = s.find('a', class_='larger bold').text.strip().encode('utf-8')
        cat = '-'.join([x.text.strip().encode('utf-8') for x in s.find_all('a', class_='darkgrey')])

        day, week, month = [re.sub('[^0-9]','', x.text.strip().split(' ')[0])
                            for x in s.find('table', class_='data width100p').find('tr', class_='rowLight').find_all('td')[2:]]

        return {'url': site, 'name': name, 'category': cat, 'info': info,
                'iua_day': day, 'iua_week': week, 'iua_month': month}
    except Exception as e:
        print e
        print url


def parse_cat_iua(url):
    r = requests.get(url, headers=get_head, verify=False)
    s = BeautifulSoup(r.text, 'lxml')
    return ['http://catalog.i.ua' + x['href'] for x in s.select('ol a[href^="/stat/"]')]


def get_cat_urls(url):
    out = []
    r = requests.get('http://catalog.i.ua' + url, headers=get_head, verify=False)
    s = BeautifulSoup(r.text, 'lxml')
    last_page = int(s.find('p', class_='clear Paging').find_all('a')[-2]['href'].split('&p=')[1])
    for i in range(last_page):
        out.append('http://catalog.i.ua%s&p=%d' % (url, i))
    return out


def grab_iua():
    r = requests.get('http://catalog.i.ua/', headers=get_head, verify=False)
    s = BeautifulSoup(r.text, 'lxml')
    cats = [x.find('a')['href'] for x in s.find_all('dt')]

    pool = Pool(len(cats))
    cat_urls = pool.map(get_cat_urls, cats)
    pool.close()
    pool.join()

    cat_urls = [item for sublist in cat_urls for item in sublist]

    print len(cat_urls)

    pool = Pool(10)
    urls = pool.map(parse_cat_iua, cat_urls)
    pool.close()
    pool.join()

    urls = [item for sublist in urls for item in sublist]

    print len(urls)

    pool = Pool(10)
    sites = pool.map(parse_item_iua, urls)
    pool.close()
    pool.join()

    client = MongoClient('mongodb://localhost:27017/')
    db = client['rate_db']
    db_sites = db['sites']

    for x in sites:
        if x:
            db_sites.update({'url': x['url']}, {'$set': x}, upsert=True)


# ------------------------------ meta

def parse_cat_meta(arr):
    out = []
    url, cat = arr
    r = requests.get(url, headers=get_head)
    s = BeautifulSoup(r.content, 'lxml')
    for x in s.find_all('div', class_='one_site')[:-1]:
        a = x.find_all('a')[1]
        name = a.text.strip()
        site = a['href'].split('?')[1].encode('utf-8')[:-1].replace('.www', '')
        info = x.find('div', class_='descr').text.strip()
        reg = x.find('div', class_='info').text.strip().split(' - ')[1]

        out.append({'url': site, 'name': name, 'category': cat, 'info': info, 'region': reg})
    return out


def grab_meta():
    st = time.time()
    r = requests.get('http://dir.meta.ua/ru/', headers=get_head)
    s = BeautifulSoup(r.content, 'lxml')
    cats = [[x['href'], int(x['title'].split('- ')[1].split(' ')[0])/10 + 1, x.text.strip()]for x in s.select('table.root_dir td div > a')]

    print cats
    cat_urls = []

    for x in cats:
        for i in range(x[1]):
            cat_urls.append(['http://dir.meta.ua%s%d/' % (x[0], i), x[2]])

    pool = Pool(10)
    sites = pool.map(parse_cat_meta, cat_urls)
    pool.close()
    pool.join()

    sites = [item for sublist in sites for item in sublist]
    print len(sites)
    print sites[-1]

    print time.time() - st

    client = MongoClient('mongodb://localhost:27017/')
    db = client['rate_db']
    db_sites = db['sites']

    for x in sites:
        if x:
            db_sites.update({'url': x['url']}, {'$set': x}, upsert=True)


if __name__ == '__main__':
    freeze_support()
    # grab_bg()

    # grab_iua()

    grab_meta()
