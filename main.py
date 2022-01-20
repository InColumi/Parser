import urllib.request
from bs4 import BeautifulSoup
import os
from joblib import Parallel, delayed
import multiprocessing
from time import time
import requests
import urllib.request
import json
import csv

def get_text_html(link):
    return BeautifulSoup(requests.get(url=link).text, 'html.parser')

def get_pages_by_author(data):
    pages = {}
    name_author = ''
    for item in data:
        tag_name = item.name
        if tag_name == 'h2':
            name_author = item.text.replace(' ¶','')
        elif tag_name == 'ul':
            name_book = item.text
            links_book = []
            for i in item.findChildren('li', recurdive=False):
                if 'English' in i.text:
                    links_on_book = i.find_all('a')
                    for link in links_on_book:
                        if 'ebooks' in link['href']:
                            links_book.append(f"https://www.gutenberg.org{link['href']}")
            if len(links_book) > 0:
                pages[name_author] = links_book
    return pages

def get_direct_reference(link):
    result = 'empty'
    page = get_text_html(link)
    data = page.find("table", {"class": "files"}).findChildren('tr')
    for i in data:
        attributes = i.attrs
        if 'about' in attributes:
            if 'txt' in i['about']:
                result = i['about']
    return result

# def download_by_link(link):
#     urllib.request.urlretrieve(keu_for_download, dir_name + '/' + link[key])

def get_linc_from_dict(pages):
    count = 1
    for key in pages.keys():
        a_pool = multiprocessing.Pool()
        res = a_pool.map(get_direct_reference, pages[key])
        print(res)
        if count <= 10:
            count += 1
        else:
            break

def get_dictionary(tags):
    name = ''
    result = {}
    for tag in tags:
        if tag.name == 'th':
            name = tag.text
        elif tag.name == 'td' and name != '':
            result = {name: tag.text.rstrip().lstrip()}
    return result

def get_list_dictionary(link):
    list_info = []
    page = get_text_html(link)
    data = page.find("table", {"class": "bibrec"}).find_all('tr')
    for tr in data:
        dictionary = get_dictionary(tr)
#         print(dictionary)
        if len(dictionary) > 0:
            res = 'Downloads' in dictionary
            if res == False:
                list_info.append(dictionary)
    return list_info

def delete_duplicate(list_dict):
    keys = []
    for item in list_dict:
        keys.append(list(item.keys())[0])
    unique_keys = set(keys)
    new_dict = {}
    for key in unique_keys:
        new_list = []
        for item in list_dict:
            #print(key, item, key in item)
            if key in item:
                new_list.append(item[key])
        new_dict[key] = new_list
    return new_dict

def get_raw_data(pages):
    print(len(pages))
    list_dictionaty_without_duplicate = []
    count = 0
    for key in pages.keys():
        print(count, key)
        for item in pages[key]:
            list_dictionary = get_list_dictionary(item)
            list_dictionaty_without_duplicate.append(delete_duplicate(list_dictionary))
        count += 1
        if count > 50:
            break
    return list_dictionaty_without_duplicate

def get_json(dictionary):
    return json.dumps(dictionary, indent = 0)

def create_csv(name, data):
    header = ['Author', 'Title']
    with open(name, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for i in data:
            if 'Author' in i:
                for author in i['Author']:
                    for title in i['Title']:
                        writer.writerow([author, title])
            if 'Contributor' in i:
                for contributor in i['Contributor']:
                    for title in i['Title']:
                        writer.writerow([contributor, title])



url = "https://www.gutenberg.org/browse/authors/a"
page = get_text_html(url)
pages = get_pages_by_author(page.find("div", {"class": "pgdbbyauthor"}).find('div').children)

get_linc_from_dict(pages)
raw_data = get_raw_data(pages)
create_csv('parsed_data.csv', raw_data)