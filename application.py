import random
from fastapi import FastAPI
import uvicorn
from fastapi.responses import HTMLResponse, FileResponse
from elasticsearch import AsyncElasticsearch
from typing import Optional
import json
from htmldocx import HtmlToDocx
from fastapi.middleware.cors import CORSMiddleware


application = FastAPI()
application.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
es = AsyncElasticsearch(
    hosts=[{'host': 'vpc-debateev2-rh4osogaj2xrcjufnehcrce7hm.us-west-1.es.amazonaws.com', 'port': 443}],
    use_ssl=True
)


@application.get("/api/v1/search")
async def search(q: str, p: int, year: Optional[str] = None, dtype: Optional[str] = None):
    amt = 20
    if year and dtype:
        years = year.split(",")
        body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
            "tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}, {"terms": {"year": years}}]}}}
        res = await es.search(index=str(dtype), from_=(
            int(p)*amt), size=amt, doc_type="cards", track_total_hits=True, body=body)
    elif year:
        years = year.split(",")
        body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
            "tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}, {"terms": {"year": years}}]}}}
        res = await es.search(index="openev,ld,college,hspolicy", from_=(
            int(p)*amt), size=amt, doc_type="cards", track_total_hits=True, body=body)
    elif dtype:
        body = {"query": {"multi_match": {"query": q, "fields": [
            "tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}}
        res = await es.search(index=dtype, doc_type="cards", from_=(int(p)*amt), track_total_hits=True, size=amt,
                              body=body)
    else:
        res = await es.search(index="openev,ld,college,hspolicy", doc_type="cards", from_=(int(p)*amt), track_total_hits=True,
                              size=amt, body={"query": {"multi_match": {"query": q, "fields": ["tag^2", "cardHtml"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}})
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
                await es.delete_by_query(index="_all", doc_type="cards", wait_for_completion=False, body={
                    "query": {"match_phrase": {"_id": doc['_id']}}})
    except KeyError:
        pass
    return results


@application.get("/api/v1/autocomplete")
async def autocomplete(q: str, dtype: Optional[str] = None, year: Optional[str] = None):
    amt = 5
    if year and dtype:
        years = year.split(",")
        body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
                "tag^2", "cardHtml"], "operator": "and"}}, {"terms": {"year": years}}]}}, "fields": ["tag", "cite"]}
        res = await es.search(index=str(dtype), from_=(int(0)*amt), size=amt,
                              doc_type="cards", track_total_hits=True, body=body)
    elif year:
        years = year.split(",")
        body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
            "tag^2", "cardHtml"], "operator": "and"}}, {"terms": {"year": years}}]}}, "fields": ["tag", "cite"]}
        res = await es.search(index="openev,ld,college,hspolicy", from_=(
            int(0)*amt), size=amt, doc_type="cards", track_total_hits=True, body=body)
    elif dtype:
        res = await es.search(index=str(dtype), doc_type="cards", from_=(int(0)*amt), track_total_hits=True, size=amt, body={
            "query": {"multi_match": {"query": q, "fields": ["tag^2", "cardHtml"], "operator": "and"}}, "fields": ["tag", "cite"]})
    else:
        res = await es.search(index="openev,ld,college,hspolicy", doc_type="cards", from_=(int(0)*amt), track_total_hits=True,
                              size=amt, body={"query": {"multi_match": {"query": q, "fields": ["tag^2", "cardHtml"], "operator": "and"}}, "fields": ["tag", "cite"]})

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


@application.get('/api/v1/cards/imfeelinglucky')
async def imfeelinglucky():
    results = {}
    res = await es.search(index="openev,ld,college,hspolicy,usersubmit", doc_type="cards", body={"size": 1, "query": {"function_score": {
        "functions": [{"random_score": {"seed": ''.join(["{}".format(random.randint(0, 9)) for num in range(0, 13)])}}]}}})
    for doc in res['hits']['hits']:
        results['_source'] = (doc['_id'], doc['_source'],
                              'dtype: ' + doc['_index'])
    return results


@application.get("/api/v1/cards/{cardid}")
async def get_card(cardid: str):
    res = await es.search(index="_all", body={
        "query": {"match_phrase": {"_id": cardid}}})
    i = 0
    results = {}
    results['hits'] = res['hits']['total']['value']

    for doc in res['hits']['hits']:
        results['_source' + str(i)] = (doc['_id'],
                                       doc['_source'], 'dtype: ' + doc['_index'])
        i += 1

    return results


@application.get("/api/v1/saved")
async def saved(q: str):
    cardid = q.split(',')
    search_arr = []
    for i in range(len(cardid)):
        search_arr.append({'index': '_all'})
        search_arr.append(
            {"query": {"match_phrase": {"_id": cardid[i]}}})
    req = ''
    for each in search_arr:
        req += '%s \n' % json.dumps(each)
    res = await es.msearch(body=req)
    x = {}
    i = 0
    for card in res['responses']:
        x['_source' + str(i)] = (card['hits']['hits'][0]['_id'], card['hits']
                                 ['hits'][0]['_source'], 'dtype: ' + card['hits']['hits'][0]['_index'])
        i += 1
    return x


@application.get("/", response_class=HTMLResponse)
async def root():
    return '<h1>Welcome to the DebateEV API</h1><p>If you came here by accident, go to <a href="http://debatev.com">the main site</a></p>'


@application.get('/api/v1/download')
async def download(q: str):
    cardid = q.split(',')
    search_arr = []
    for i in range(len(cardid)):
        search_arr.append({'index': '_all'})
        search_arr.append(
            {"query": {"match_phrase": {"_id": cardid[i]}}})
    req = ''
    for each in search_arr:
        req += '%s \n' % json.dumps(each)
    res = await es.msearch(body=req)
    a = ""
    for card in res['responses']:
        a += card['hits']['hits'][0]['_source']['cardHtml']
    new_parser = HtmlToDocx()

    docx = new_parser.parse_html_string(a)
    docx.save('test.docx')
    return FileResponse('test.docx')


if __name__ == "__main__":
    uvicorn.run("application:application", reload=True)
