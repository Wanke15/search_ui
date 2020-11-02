import json
from datetime import datetime
from flask import Flask, jsonify, request, render_template, redirect, url_for, Response
from elasticsearch import Elasticsearch

import jieba_fast
jieba_fast.initialize()

# es = Elasticsearch(hosts='localhost:9292')
es = Elasticsearch(hosts='http://portal.int.zuzuche.info:20019/')

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def search():
    if request.method == 'GET':
        return render_template('index.html', hits=None)
    if request.method == 'POST':
        keyword = request.form['keyword']
        hits_size = request.form['top_k']
        if not hits_size:
            hits_size = 5
        # keyword = '张家界'
        # keyword = request.args.get('keyword')

        # body = {
        #     "query": {
        #         "multi_match": {
        #             "query": keyword,
        #             "fields": ["title^10", "content"]
        #         }
        #     }
        # }

        # body = {
        #     "query": {
        #         "match_phrase_prefix": {
        #             "title": {
        #                 "query": keyword
        #             }
        #         }
        #     }
        # }

        should_queries = [{
                            "match_phrase_prefix": {
                                "title": {
                                    "query": keyword
                                }
                            }
                        }]
        [should_queries.append({
                            "match_phrase_prefix": {
                                "title": {
                                    "query": kw
                                }
                            }
                        }) for kw in jieba_fast.lcut(keyword)]

        body = {
            "query": {
                "bool": {
                    "should": should_queries
                }
            }
        }

        # body = {
        #     "query": {
        #         "bool": {
        #             "must": [
        #                 {
        #                     "query_string": {
        #                         "query": keyword,
        #                         "fields": ["title^10", "content"]
        #                     }
        #                 }
        #             ]
        #         }
        #     }
        # }

        res = es.search(index="zzc_home_page_data", body=body, size=hits_size)
        print(res['hits']['hits'])

        # return jsonify(res['hits']['hits'])
        return render_template('index.html', hits=res['hits']['hits'], hits_size=int(hits_size))


@app.route('/recommend', methods=['GET', 'POST'])
def recommend():
    if request.method == 'GET':
        if request.args.get('hits') is None:
            return render_template('index.html', hits=None)
    if request.method == 'POST':
        keyword = request.form['keyword']
        recs_size = request.form['top_k']
        if not recs_size:
            recs_size = 5
        recs_size = int(recs_size)

        recs, res = inner_recommend(keyword, recs_size)

        return render_template('detail.html', hits=res['hits']['hits'], recs_size=len(recs), recs=recs)


def inner_recommend(keyword, rec_size=3):
    if not rec_size:
        rec_size = 5
    body = {
        "query": {
            "multi_match": {
                "query": keyword,
                "fields": ["cn_name^10", "py_name", "en_name^10", "memo", "address", "tags", "city_cn", "city_en"]
            }
        }
    }
    res = es.search(index="poi", body=body, size=rec_size + 1)
    recs = [{'pic': _r['_source']['pic'], 'cn_name': _r['_source']['cn_name']} for _r in res['hits']['hits']]
    return recs, res


@app.route('/just_recommend/<name>', methods=['GET'])
def just_recommend(name, rec_size=5):
    keyword = name
    if not rec_size:
        rec_size = 5
    recs_size = int(rec_size)

    recs, res = inner_recommend(keyword, recs_size)

    return render_template('recommend.html', current_item=res['hits']['hits'][0]['_source'], recs_size=len(recs),
                           recs=recs)


@app.route('/es_search', methods=['GET'])
def es_correct():
    keyword = request.args.get('text')
    body = {
        "query": {
            "multi_match": {
                "query": keyword,
                "fields": ["cn_name"]
            }
        }
    }

    result = es.search(index="poi", body=body, size=3)

    if result['hits']['hits']:
        final_hit = result['hits']['hits'][0]['_source']
        final_hit = {'ES Match结果': final_hit.get('cn_name')}
        return Response(json.dumps(final_hit, ensure_ascii=False), content_type='application/json')
    else:
        return Response(json.dumps({'msg': 'No record found!'}), content_type='application/json')


def single_query(text):
    body = {
        "query": {
            "multi_match": {
                "query": text,
                "fields": ["cn_name"]
            }
        }
    }

    result = es.search(index="poi", body=body, size=1)

    if result['hits']['hits']:
        final_hit = result['hits']['hits'][0]['_source']
        final_hit = final_hit.get('cn_name')
        return final_hit
    else:
        return ''


@app.route('/es_search_batch', methods=['POST'])
def es_correct_batch():
    req_data = json.loads(request.data.decode("utf-8"))
    keywordS = req_data.get('texts')

    final_hits = [{'搜索文本': _txt, '命中结果': single_query(_txt)} for _txt in keywordS]

    return Response(json.dumps(final_hits, ensure_ascii=False), content_type='application/json')


if __name__ == '__main__':
    app.run(port=5000, debug=True)
