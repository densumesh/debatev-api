import requests
import time
import pypandoc
from bs4 import BeautifulSoup
import hashlib
from elasticsearch import Elasticsearch
import json
import os
import urllib3
urllib3.disable_warnings()


es = Elasticsearch(
    hosts=[{'host': 'vpc-debateev2-rh4osogaj2xrcjufnehcrce7hm.us-west-1.es.amazonaws.com', 'port': 443}],
    use_ssl=True
)


def converttoHTML(filepath, filelink, types, year):
    output = pypandoc.convert_file(filepath, 'html')

    with open('test.html', 'w') as fp:
        fp.write(output)
    num_of_cards = 1
    allHtml = {}
    with open('test.html') as fp:
        soup = BeautifulSoup(fp, "lxml")
        all_card_tags = soup.find_all('h4')
        for h4 in all_card_tags:
            try:
                abstract = h4
                citation = h4.find_next("p")
                card = h4.find_next("p").find_next("p")
                full_doc = card
                doc_word_length = len(full_doc.text.split())
                if doc_word_length >= 70:
                    allHtml["card " + str(num_of_cards)] = [{"tag": str(abstract), "cite": str(citation), "cardHtml": str(
                        abstract) + str(citation) + str(full_doc), "filepath": filelink, "dtype": types, "year": year}]
                    num_of_cards += 1
            except AttributeError as e:
                print("a card was skipped because " + str(e))
                pass
    return allHtml


def uploadcase(z, dtype):
    for i in range(len(z)):
        tag = z['card ' + str(i + 1)][0]['tag']
        cardHtml = z['card ' + str(i + 1)][0]['cardHtml']
        cite = z['card ' + str(i + 1)][0]['cite']
        filepath = z['card ' + str(i + 1)][0]['filepath']
        year = z['card ' + str(i + 1)][0]['year']
        x = hashlib.sha224(bytes(tag, 'utf-8')).hexdigest()
        es.index(index=dtype, doc_type='cards', id=x, body={
            "tag": tag,
            "cite": cite,
            "cardHtml": cardHtml,
            "filepath": filepath,
            "year": year
        })


cookies = {
    'caselist_token': 'e04af4d2ecb40830ded22642743f3f16',
}

headers = {
    'authority': 'api.opencaselist.com',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,es;q=0.8',
    'cache-control': 'no-cache',
    # 'cookie': 'caselist_token=e04af4d2ecb40830ded22642743f3f16',
    'dnt': '1',
    'pragma': 'no-cache',
    'referer': 'https://api.opencaselist.com/',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
}


def updatecases():
    r = requests.get(
        f'https://api.opencaselist.com/v1/caselists/hspolicy22/recent', headers=headers, cookies=cookies)
    if r:
        data = json.loads(r.text)
        for file in data:
            if(file['opensource']):
                print(file['opensource'])
                try:
                    response = requests.get('https://api.opencaselist.com/v1/download?path=' + file['opensource'],
                                            cookies=cookies, headers=headers)

                    os.makedirs(os.path.dirname(
                        'hspolicy_files/' + file['opensource']), exist_ok=True)

                    with open('hspolicy_files/' + file['opensource'], 'wb') as f:
                        f.write(response.content)
                        z = converttoHTML(
                            'hspolicy_files/' + file['opensource'], 'https://api.opencaselist.com/v1/download?path=' + file['opensource'], 'OpenEv', 2022)
                        uploadcase(z, 'hspolicy')
                        os.remove('hspolicy_files/' +
                                  file['opensource'])
                except Exception as e:
                    print(e)
                    time.sleep(60)

    r = requests.get(
        f'https://api.opencaselist.com/v1/caselists/hsld22/recent', headers=headers, cookies=cookies)
    if r:
        data = json.loads(r.text)
        for file in data:
            if(file['opensource']):
                print(file['opensource'])
                try:
                    response = requests.get('https://api.opencaselist.com/v1/download?path=' + file['opensource'],
                                            cookies=cookies, headers=headers)

                    os.makedirs(os.path.dirname(
                        'hsld_files/' + file['opensource']), exist_ok=True)
                    with open('hsld_files/' + file['opensource'], 'wb') as f:
                        f.write(response.content)
                        z = converttoHTML(
                            'hsld_files/' + file['opensource'], 'https://api.opencaselist.com/v1/download?path=' + file['opensource'], 'OpenEv', 2022)
                        uploadcase(z, 'ld')
                        os.remove('hsld_files/' +
                                  file['opensource'])
                except Exception as e:
                    print(e)
                    time.sleep(60)

    r = requests.get(
        f'https://api.opencaselist.com/v1/caselists/ndtceda22/recent', headers=headers, cookies=cookies)
    if r:
        data = json.loads(r.text)
        for file in data:
            if(file['opensource']):
                print(file['opensource'])
                try:
                    response = requests.get('https://api.opencaselist.com/v1/download?path=' + file['opensource'],
                                            cookies=cookies, headers=headers)

                    os.makedirs(os.path.dirname(
                        'college_files/' + file['opensource']), exist_ok=True)
                    with open('college_files/' + file['opensource'], 'wb') as f:
                        f.write(response.content)
                        z = converttoHTML(
                            'college_files/' + file['opensource'], 'https://api.opencaselist.com/v1/download?path=' + file['opensource'], 'OpenEv', 2022)
                        uploadcase(z, 'college')
                        os.remove('college_files/' +
                                  file['opensource'])
                except Exception as e:
                    print(e)
                    time.sleep(60)


updatecases()
print('done')
