import os
from elasticsearch import Elasticsearch
from htmldocx import HtmlToDocx
import random
import flask
import urllib3
from flask import request
from flask_cors import CORS, cross_origin
import json
from werkzeug.utils import secure_filename
import pypandoc
from pypandoc.pandoc_download import download_pandoc
from bs4 import BeautifulSoup
import hashlib
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address


urllib3.disable_warnings()

application = flask.Flask(__name__)
cors = CORS(application, resources={r"/api/v1/*": {"origins": "*"}})
application.config['CORS_HEADERS'] = 'Content-Type'
application.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000
UPLOAD_FOLDER = '/'
application.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'txt', 'pdf', "docx", "doc"}
limiter = Limiter(
    application,
    key_func=get_remote_address
)


es = Elasticsearch(
    hosts=[{'host': 'vpc-debateev2-rh4osogaj2xrcjufnehcrce7hm.us-west-1.es.amazonaws.com', 'port': 443}],
    use_ssl=True
)


def converttoHTML(filepath, filelink, types, year):
    try:
        output = pypandoc.convert_file(filepath, 'html')
    except:
        download_pandoc()
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


@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
@application.route('/api/v1/search', methods=['GET'])
def search():
    """
    Have rogas elasticsearch stuff here and search for a card and return tag, HTMLtext, download link
    """
    amt = 20
    if 'q' in request.args:
        id = request.args['q']
    else:
        return "<h1>400</h1> <p>No query field provided. Please specify an query.</p>", 400

    if 'p' in request.args:
        page = int(request.args['p'])
    else:
        return "<h1>400</h1><p> No page field provided. Please specify a page.</p>", 400

    if 'amt' in request.args:
        if not int(request.args['amt']) > 70:
            amt = int(request.args['amt'])

    if 'year' in request.args and 'dtype' in request.args:
        year = request.args['year'].split(',')
        dtype = request.args['dtype']
        body = {"query": {"bool": {"must": [{"multi_match": {"query": id, "fields": [
            "tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO"}}, {"terms": {"year": year}}]}}}
        if dtype == "usersubmit":
            res = es.search(index="openev,ld,college,hspolicy,usersubmit", doc_type="cards", from_=(int(page)*amt), track_total_hits=True,
                            size=amt, body={"query": {"multi_match": {"query": id, "fields": ["tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO"}}})
        else:
            res = es.search(index=str(dtype), from_=(
                int(page)*amt), size=amt, doc_type="cards", track_total_hits=True, body=body)

    elif 'year' in request.args:
        year = request.args['year'].split(',')
        body = {"query": {"bool": {"must": [{"multi_match": {"query": id, "fields": [
            "tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO"}}, {"terms": {"year": year}}]}}}
        res = es.search(index="openev,ld,college,hspolicy", from_=(
            int(page)*amt), size=amt, doc_type="cards", track_total_hits=True, body=body)

    elif 'dtype' in request.args:
        dtype = request.args['dtype']
        if dtype == "usersubmit":
            res = es.search(index="openev,ld,college,hspolicy,usersubmit", doc_type="cards", from_=(int(page)*amt), track_total_hits=True,
                            size=amt, body={"query": {"multi_match": {"query": id, "fields": ["tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO"}}})
        else:
            res = es.search(index=str(dtype), doc_type="cards", from_=(int(page)*amt), track_total_hits=True, size=amt,
                            body={"query": {"multi_match": {"query": id, "fields": ["tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO"}}})

    else:
        res = es.search(index="openev,ld,college,hspolicy", doc_type="cards", from_=(int(page)*amt), track_total_hits=True,
                        size=amt, body={"query": {"multi_match": {"query": id, "fields": ["tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO"}}})

    tags = []
    cite = []
    results = {}
    i = 0
    results['hits'] = res['hits']['total']['value']
    try:
        for doc in res['hits']['hits']:
            if doc['_source']['tag'] not in tags and doc['_source']['cite'] not in cite:
                tags.append(doc['_source']['tag'])
                cite.append(doc['_source']['cite'])
                results['_source' + str(i)] = (doc['_id'],
                                               doc['_source'], 'dtype: ' + doc['_index'])
                i += 1
            else:
                es.delete_by_query(index="_all", doc_type="cards", wait_for_completion=False, body={
                                   "query": {"match_phrase": {"_id": doc['_id']}}})
    except KeyError:
        pass

    return results


@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
@application.route('/api/v1/autocomplete', methods=['GET'])
def autocomplete():
    """
    Have rogas elasticsearch stuff here and search for a card and return tag, HTMLtext, download link
    """
    amt = 5
    if 'q' in request.args:
        id = request.args['q']
    else:
        return "<h1>400</h1> <p>No query field provided. Please specify an query.</p>", 400

    if 'year' in request.args and 'dtype' in request.args:
        year = request.args['year'].split(',')
        dtype = request.args['dtype']
        body = {"query": {"bool": {"must": [{"multi_match": {"query": id, "fields": [
                "tag^2", "cardHtml"], "operator": "and"}}, {"terms": {"year": year}}]}}}
        res = es.search(index=str(dtype), from_=(int(0)*amt), size=amt,
                        doc_type="cards", track_total_hits=True, body=body)

    elif 'year' in request.args:
        year = request.args['year'].split(',')
        body = {"query": {"bool": {"must": [{"multi_match": {"query": id, "fields": [
            "tag^2", "cardHtml"], "operator": "and"}}, {"terms": {"year": year}}]}}}
        res = es.search(index="openev,ld,college,hspolicy", from_=(
            int(0)*amt), size=amt, doc_type="cards", track_total_hits=True, body=body)

    elif 'dtype' in request.args:
        dtype = request.args['dtype']
        res = es.search(index=str(dtype), doc_type="cards", from_=(int(0)*amt), track_total_hits=True, size=amt, body={
            "query": {"multi_match": {"query": id, "fields": ["tag^2", "cardHtml"], "operator": "and"}}})
    else:
        res = es.search(index="openev,ld,college,hspolicy", doc_type="cards", from_=(int(0)*amt), track_total_hits=True,
                        size=amt, body={"query": {"multi_match": {"query": id, "fields": ["tag^2", "cardHtml"], "operator": "and"}}})

    tags = []
    cite = []
    results = {}
    i = 0
    try:
        for doc in res['hits']['hits']:
            if doc['_source']['tag'] not in tags and doc['_source']['cite'] not in cite:
                tags.append(doc['_source']['tag'])
                cite.append(doc['_source']['cite'])
                results['_source' + str(i)] = (doc['_id'],
                                               doc['_source']['tag'], 'dtype: ' + doc['_index'])
                i += 1
            else:
                es.delete_by_query(index="_all", doc_type="cards", wait_for_completion=False, body={
                                   "query": {"match_phrase": {"_id": doc['_id']}}})
    except KeyError:
        pass

    return results


@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
@application.route('/api/v1/cards/<cardid>', methods=['GET'])
def getcards(cardid):
    """
    Use es module in order to get a certain card directly and return its details
    """

    res = es.search(index="_all", doc_type="cards", body={
                    "query": {"match_phrase": {"_id": cardid}}})
    return res


@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
@application.route('/api/v1/saved', methods=['GET'])
def saved():
    if 'q' in request.args:
        cardid = request.args['q']
    else:
        return "<h1>400</h1> <p>No query field provided. Please specify an query.</p>", 400
    cardid = cardid.split(',')
    search_arr = []
    for i in range(len(cardid)):
        search_arr.append({'index': '_all'})
        search_arr.append({"query": {"match_phrase": {"_id": cardid[i]}}})
    req = ''
    for each in search_arr:
        req += '%s \n' % json.dumps(each)
    res = es.msearch(body=req)
    x = {}
    i = 0
    for card in res['responses']:
        x['_source' + str(i)] = (card['hits']['hits'][0]['_id'], card['hits']
                                 ['hits'][0]['_source'], 'dtype: ' + card['hits']['hits'][0]['_index'])
        i += 1
    return x


@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
@application.route('/api/v1/cards/imfeelinglucky', methods=['GET'])
def randomcard():
    """
    Gets a random card from the database for "I'm feeling lucky"
    """
    results = {}
    res = es.search(index="openev,ld,college,hspolicy,usersubmit", doc_type="cards", body={"size": 1, "query": {"function_score": {
                    "functions": [{"random_score": {"seed": ''.join(["{}".format(random.randint(0, 9)) for num in range(0, 13)])}}]}}})
    for doc in res['hits']['hits']:
        results['_source'] = (doc['_id'], doc['_source'],
                              'dtype: ' + doc['_index'])
    return results


@application.route('/', methods=['GET'])
def home():
    return '<h1>Welcome to the DebateEV API</h1><p>If you came here by accident, go to <a href="http://debatev.com">the main site</a></p>'


@application.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@application.route('/api/v1/download', methods=['GET'])
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def download():
    if 'q' in request.args:
        cardid = request.args['q']
    else:
        return "<h1>400</h1> <p>No query field provided. Please specify an query.</p>", 400
    cardid = cardid.split(',')
    search_arr = []
    for i in range(len(cardid)):
        search_arr.append({'index': '_all'})
        search_arr.append({"query": {"match_phrase": {"_id": cardid[i]}}})
    req = ''
    for each in search_arr:
        req += '%s \n' % json.dumps(each)
    res = es.msearch(body=req)
    a = ""
    for card in res['responses']:
        a += card['hits']['hits'][0]['_source']['cardHtml']
    new_parser = HtmlToDocx()

    docx = new_parser.parse_html_string(a)
    docx.save('test.docx')
    return flask.send_file('test.docx')


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@application.route('/api/v1/uploadCase', methods=['POST'])
@limiter.limit("10/hour")
def uploadCase():
    if 'file' not in request.files:
        return "No file part", 400
    file = request.files['file']
    # If the user does not select a file, the browser submits an
    # empty file without a filename.
    if file.filename == '':
        return 'No selected file', 400
    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            file.save(filename)
            html = converttoHTML(
                filename, "https://www.debatev.com/" + file.filename, "usersubmit", 2021)
            uploadcase(html,  "usersubmit")
            os.remove(filename)
            return "Uploaded " + str(filename), 200
        except Exception as e:
            return "Error: " + str(e), 400


if __name__ == "__main__":
    application.debug = True
    application.run()
