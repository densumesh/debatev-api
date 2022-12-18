import random
from fastapi import FastAPI, HTTPException
import uvicorn
from fastapi.responses import HTMLResponse, FileResponse
from elasticsearch import AsyncElasticsearch
from typing import Optional
import json
from htmldocx import HtmlToDocx
from fastapi.middleware.cors import CORSMiddleware
import logging
import socket
from logging.handlers import SysLogHandler



class ContextFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = ContextFilter.hostname
        return True


syslog = SysLogHandler(address=('logs3.papertrailapp.com', 28701))
syslog.addFilter(ContextFilter())
format = '%(asctime)s %(hostname)s DEBATEV_BACKEND: %(message)s'
formatter = logging.Formatter(format, datefmt='%b %d %H:%M:%S')
syslog.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(syslog)
logger.setLevel(logging.WARNING)
logger.info("This is a message")

tags_metadata = [
    {
        "name": "search",
        "description": "Search for a query in the Elasticsearch database",
    },
    {
        "name": "autocomplete",
        "description": "Autocomplete search queries as you type",
    },
    {
        "name": "imfeelinglucky",
        "description": "Get a random card from the database",
    },
    {
        "name": "get_card",
        "description": "Get a specific card from the database",
    },
    {
        "name": "saved",
        "description": "Get saved cards",
    },
    {
        "name": "download",
        "description": "Download a card as a Word document",
    }
]


application = FastAPI(title="Debate Evidence API",
                      description="A REST API for the Debate Evidence project", openapi_tags=tags_metadata)
application.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
es = AsyncElasticsearch(
    hosts=[{'host': 'vpc-debatev-r64moafdhze4axbpwde4jzrdwi.us-west-1.es.amazonaws.com', 'port': 443}],
    use_ssl=True,
    timeout=60
)


@application.get("/api/v1/search", tags=["search"],
                 responses={
    200: {
        "description": "Results by query",
        "content": {
            "application/json": {
                "example": {
                    "_source0": [
                        "CARD_ID",
                        {
                            "tag": "Tag with HTML formatting",
                            "cite": "Cite with HTML formatting",
                            "cardHtml": "Full text of response with HTML formatting",
                            "filepath": "Link to orginal document",
                            "year": "Year of card"
                        },
                        "dtype: Index of document"
                    ]}
            }
        },
    },
},)
async def search(q: str, p: int, year: Optional[str] = None, dtype: Optional[str] = "college,hspolicy,collegeld,ld,openev", order: Optional[str] = None):
    try:
        amt = 20
        if year:
            years = year.split(",")
            if order == "year":
                body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
                    "tag^2", "cardHtml", "cite"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}, {"terms": {"year.keyword": years}}]}}, "sort": [{"year": {"order": "desc"}}, "_score"]}
            else:
                body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
                    "tag^2", "cardHtml", "cite"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}, {"terms": {"year.keyword": years}}]}}}
            res = await es.search(index=dtype, from_=(
                int(p)*amt), size=amt, track_total_hits=True, body=body)
        else:
            if order == "year":
                body = {"query": {"multi_match": {"query": q, "fields": [
                    "tag^2", "cardHtml", "cite"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}, "sort": [{"year.keyword": {"order": "desc"}}, "_score"]}
            else:
                body = {"query": {"multi_match": {"query": q, "fields": [
                    "tag^2", "cardHtml", "cite"], "operator": "and", "fuzziness": "AUTO", "prefix_length": 1}}}
            res = await es.search(index=dtype, from_=(int(p)*amt), track_total_hits=True,
                                size=amt, body=body)
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
                                                doc['_source'], 'dtype: ' + doc['_index'])
                    i += 1
                else:
                    await es.delete_by_query(index="college,hspolicy,collegeld,ld,openev", wait_for_completion=False, body={
                        "query": {"match_phrase": {"_id": doc['_id']}}})
        except KeyError:
            pass
        results['hits'] = res['hits']['total']['value']
        return results
    except Exception as e:
        logging.error(e)
        logging.error("The query parameters were " + q + " " + p + " " + year + " " + dtype)
        raise HTTPException(status_code=500, detail="Search Timed Out")

@application.get("/api/v1/autocomplete", tags=["autocomplete"],
                 responses={
    200: {
        "description": "Autocomplete search queries as you type",
        "content": {
            "application/json": {
                "example": {
                    "_source0": [
                        "CARD_ID",
                        "Tag with HTML formatting",
                        "dtype: Index of document"
                    ]}
            }
        },
    },
})
async def autocomplete(q: str, dtype: Optional[str] = "college,hspolicy,collegeld,ld,openev", year: Optional[str] = None):
    try:
        amt = 5
        if year:
            years = year.split(",")
            body = {"query": {"bool": {"must": [{"multi_match": {"query": q, "fields": [
                "tag^2", "cardHtml", "cite"], "operator": "and", "fuzziness": 1, "prefix_length": 1}}, {"terms": {"year": years}}]}}, "fields": ["tag", "cite"]}
            res = await es.search(index=dtype, from_=(
                int(0)*amt), size=amt, track_total_hits=True, body=body)
        else:
            res = await es.search(index=dtype, from_=(int(0)*amt), track_total_hits=True,
                                size=amt, body={"query": {"multi_match": {"query": q, "fields": ["tag^2", "cardHtml", "cite"], "operator": "and", "fuzziness": 1, "prefix_length": 1}}, "fields": ["tag", "cite"]})

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
                    es.delete_by_query(index="college,hspolicy,collegeld,ld,openev", wait_for_completion=False, body={
                                    "query": {"match_phrase": {"_id": doc['_id']}}})
        except KeyError:
            pass

        return results
    except Exception as e:
        logging.error(e)
        logging.error("The query parameters were " + q + " " + year + " " + dtype)
        raise HTTPException(status_code=500, detail="Search Timed Out")



@application.get('/api/v1/cards/imfeelinglucky', tags=["imfeelinglucky"],
                 responses={
    200: {
        "description": "Get a random card",
        "content": {
            "application/json": {
                "example": {
                    "_source": [
                        "CARD_ID",
                        {
                            "tag": "Tag with HTML formatting",
                            "cite": "Cite with HTML formatting",
                            "cardHtml": "Full text of response with HTML formatting",
                            "filepath": "Link to orginal document",
                            "year": "Year of card"
                        },
                        "dtype: Index of document"
                    ]
                }
            },
        },
    }})
async def imfeelinglucky():
    results = {}
    res = await es.search(index="college,hspolicy,collegeld,ld,openev", body={"size": 1, "query": {"function_score": {
        "functions": [{"random_score": {"seed": ''.join(["{}".format(random.randint(0, 9)) for num in range(0, 13)])}}]}}})
    for doc in res['hits']['hits']:
        results['_source'] = (doc['_id'], doc['_source'],
                              'dtype: ' + doc['_index'])
    return results


@application.get("/api/v1/cards/{cardid}", tags=["get_card"],
                 responses={
    200: {
        "description": "Get a card by ID",
        "content": {
            "application/json": {
                "example": {
                    "_source0": [
                        "CARD_ID",
                        {
                            "tag": "Tag with HTML formatting",
                            "cite": "Cite with HTML formatting",
                            "cardHtml": "Full text of response with HTML formatting",
                            "filepath": "Link to orginal document",
                            "year": "Year of card"
                        },
                        "dtype: Index of document"
                    ]
                }
            },
        },
    }})
async def get_card(cardid: str):
    res = await es.search(index="college,hspolicy,collegeld,ld,openev", body={
        "query": {"match_phrase": {"_id": cardid}}})
    i = 0
    results = {}

    for doc in res['hits']['hits']:
        results['_source' + str(i)] = (doc['_id'],
                                       doc['_source'], 'dtype: ' + doc['_index'])
        i += 1
    results['hits'] = res['hits']['total']['value']
    return results


@application.get("/api/v1/saved", tags=["saved"], responses={
    200: {
        "description": "Get Saved Cards",
        "content": {
            "application/json": {
                "example": {
                    "_source0": [
                        "CARD_ID",
                        {
                            "tag": "Tag with HTML formatting",
                            "cite": "Cite with HTML formatting",
                            "cardHtml": "Full text of response with HTML formatting",
                            "filepath": "Link to orginal document",
                            "year": "Year of card"
                        },
                        "dtype: Index of document"
                    ]
                }
            },
        },
    }})
async def saved(q: str):
    cardid = q.split(',')
    search_arr = []
    for i in range(len(cardid)):
        search_arr.append({'index': 'college,hspolicy,collegeld,ld,openev'})
        search_arr.append(
            {"query": {"match_phrase": {"_id": cardid[i]}}})
    req = ''
    for each in search_arr:
        req += '%s \n' % json.dumps(each)
    res = await es.msearch(body=req)
    x = {}
    i = 0
    for card in res['responses']:
        try:
            x['_source' + str(i)] = (card['hits']['hits'][0]['_id'], card['hits']
                                     ['hits'][0]['_source'], 'dtype: ' + card['hits']['hits'][0]['_index'])
            i += 1
        except:
            logger.error(
                "Error in saved function with: %s and with the search term: %s" % (card, q))
            pass
    return x


@ application.get("/", response_class=HTMLResponse)
async def root():
    return '<h1>Welcome to the DebateEV API</h1><p>If you came here by accident, go to <a href="http://debatev.com">the main site</a>, or look at the <a href="http://api.debatev.com/docs">documentation</a></p>'


@ application.get('/api/v1/download', tags=["download"], responses={200: {"description": "Download a specific card as a word doc",
                                                                          "content": {FileResponse}}})
async def download(q: str):
    try:
        cardid = q.split(',')
        search_arr = []
        for i in range(len(cardid)):
            search_arr.append({'index': 'college,hspolicy,collegeld,ld,openev'})
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
    except Exception as e:
        logging.error(e)
        logging.error("The query parameters were " + q)
        raise HTTPException(status_code=500, detail="Download Failed")


@ application.on_event("shutdown")
async def app_shutdown():
    await es.close()

if __name__ == "__main__":
    uvicorn.run("application:application", reload=True)
