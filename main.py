import hashlib
import requests
import re
import datasketch
import kshingle as ks
import pymorphy2
import string
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch


def hash_text(text):
    hash_object = hashlib.md5(text.encode())
    return str(hash_object.hexdigest())


URL = 'https://microsoftportal.net/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0) Gecko/20100101 Firefox/87.0'
}

'''
----------------------------Parser-----------------------------------------
'''


def get_html(url, page):
    req = requests.get(url+'page/'+ str(page), headers=HEADERS)
    return req


def get_pages_count(html):
    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.findAll(href=re.compile("https://microsoftportal.net/page"))
    return int(pagination[-2].get_text())


def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.findAll('div', class_='news')

    comps = []
    for item in items:
        comps.append({
            'title': item.find('a').get_text(strip=True),
            'link': item.find('a').get('href'),
            'text': item.find('div', class_='news_text_in').get_text(strip=True),
            'author': item.find('div', class_='news_author').get_text(strip=True),
        })
    return comps


def parse():
    html = get_html(URL, 1)
    if html.status_code == 200:
        comps = []
        pages_count = get_pages_count(html.text)
        print("Сколько страничек парсить? max = ", pages_count)
        pages_count = int(input())
        for page in range(1, pages_count + 1):
            print(f'Парсинг страницы {page} из {pages_count} ...')
            html = get_html(URL, page)
            comps.extend(get_content(html.text))

        i = 1
        for comp in comps:
            print(i, comp['title'] + ":\n" +
                  comp['text'] + '\n' +
                  'link->' + comp['link'] + '\n' +
                  'Author: ' + comp['author'] + '\n')
            i += 1

        print(f'Получено {len(comps)} новостей')
    elif html.status_code == 404:
        print("Error 404")
    else:
        print('Error' + str(html.status_code))

    return comps


comps = parse()

'''
-----------------------Elastic------------------------------------
'''


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('This program was connected to elastic')
    else:
        print('NO connection!')
    return _es


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "news": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "russian"
                    },
                    "link": {
                        "type": "text",
                        "analyzer": "russian"
                    },
                    "author": {
                        "type": "text"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, id, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='news', id=id, body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def search(es_object, search):
    res = es_object.search(body=search)
    return res


es = connect_elasticsearch()
create_index(es, 'app')
for comp in comps:
    out = store_record(es, 'app', hash_text(comp['text']), comp)

search_object = {'_source': ['title'], 'query': {'match': {'text': 'Edge'}}}
search_content = search(es, search_object)
print(search_content)
'''
---------------------------Analyzer------------------------------------------------------
'''


def remove_stopwords(text):
    stop_words = ['это', 'как', 'так',
                  'и', 'в', 'над',
                  'к', 'до', 'не',
                  'на', 'но', 'за',
                  'то', 'с', 'ли',
                  'а', 'во', 'от',
                  'со', 'для', 'о',
                  'же', 'ну', 'вы',
                  'бы', 'что', 'кто',
                  'он', 'она']

    for word in text.split(' '):
        if word in stop_words:
            text = text.replace(word, '')
    return text


def remove_whitespace(text):
    return " ".join(text.split())


def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)


def normalization(text):
    pymorph = pymorphy2.MorphAnalyzer()
    text_norm = ""
    for word in text.split(' '):
        word = pymorph.parse(word)[0].normal_form
        text_norm += word
        text_norm += ' '
    return text_norm


def remove_numbers(text):
    result = re.sub(r'\d+', '', text)
    return result


def canonize(source):
    source = source.lower()
    source = remove_punctuation(source)
    source = remove_stopwords(source)
    source = remove_numbers(source)
    source = remove_whitespace(source)
    source = normalization(source)

    return source


print("------------Анализатор--------------------")


text_list = []
for comp in comps:
    text_list.append(canonize(comp['text']))

shingles_of_texts = []


for text in text_list:
    shingles_of_texts.append(ks.shingleset_k(text, k=3))

minhash = []
i = 0
for shingles in shingles_of_texts:
    minhash.append(datasketch.MinHash(num_perm=128))
    for shingle in shingles:
        minhash[i].update(shingle.encode('utf-8'))
    i += 1

i = 0
j = 0
while(j <= len(minhash) - 1):
    print(i+1, "-", j+1, minhash[j].jaccard(minhash[i]))
    if i == len(minhash) - 1:
        i = 0
        j += 1
        continue
    i += 1