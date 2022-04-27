import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from utils.global_constant import config
from utils.log_utils import logger

es = Elasticsearch([{'host': host, 'port': config.get("elasticsearch", "port"), "http_auth": "%s:%s" % (
    config.get("elasticsearch", "user"), config.get("elasticsearch", "passwd"))} for host in
                    config.get("elasticsearch", "host").split(",")])


def get_all_data(table, query_dsl, scroll='5m', timeout='1m'):
    es_result = helpers.scan(
        client=es,
        query=query_dsl,
        scroll=scroll,
        index=table,
        timeout=timeout
    )
    return es_result


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        print('共耗时约 {:.2f} 秒'.format(time.time() - start))
        return res

    return wrapper


@timer
def batch_data(data_list, index_name, _id_name=None):
    """ 批量写入数据 """
    if _id_name:
        action = [{
            "_index": index_name,
            "_id": data_list[i][_id_name],
            '_type': '_doc',
            "_source": data_list[i]
        } for i in range(len(data_list))]
    else:
        action = [{
            "_index": index_name,
            '_type': '_doc',
            "_source": data_list[i]
        } for i in range(len(data_list))]
    helpers.bulk(es, action)


def create_index(index_name, mapping_settings):
    if es.indices.exists(index_name):
        es.indices.delete(index_name)
    res = es.indices.create(index=index_name, body=mapping_settings)


def create_alias(index, alias):
    es.indices.put_alias(index, alias)


def delete_index(index_name):
    if es.indices.exists(index_name):
        es.indices.delete(index_name)


def delete_alias(index, alias):
    es.indices.delete_alias(index, alias)


def get_alias_index_name(alias):
    if es.indices.exists_alias(alias):
        return es.indices.get_alias(name=alias)
    else:
        return None


def refresh_index(index_name):
    es.indices.refresh(index_name)


def match_phrase_search(index_name, tag, doc_num):
    if doc_num < 0:
        doc_num = 20
    body = {
        "size": doc_num,
        "query": {
            "match_phrase": {
                "text": tag
            }
        }
    }
    result_list = []
    search_result = es.search(body, index_name)
    if "hits" in search_result and "hits" in search_result.get("hits"):
        result_list = [each["_source"]["text"] for each in search_result.get("hits").get("hits")]
    return result_list


def clean_data(index_name):
    body = {
        "query": {
            "match_all": {}
        }
    }
    es.delete_by_query(index_name, body)


def es_search(index_name, query_dsl):
    try:
        res = es.search(index=index_name,
                        body=query_dsl,
                        request_timeout=100)
        took_time = res['took']
        totle_doc = res['hits']['total']['value']
        logger.info("search {}, took {} ms, get {} docs".format(query_dsl, took_time, totle_doc))
        result = []
        for l in res["hits"]["hits"]:
            source = l["_source"]
            source['_score'] = l["_score"] if l["_score"] else 0
            result.append(source)
        return result
    except Exception as e:
        logger.error("can't search result, search index is {0}, query dsl is {1}".format(index_name, query_dsl))
        return []


def es_count(index_name, query_dsl):
    try:
        count_result = es.count(index=index_name, body=query_dsl)
        if 'count' in count_result:
            return count_result['count']
        else:
            logger.error("result not contain field count, count index is {0} ,count dsl is {1}".format(index_name, query_dsl))
            return -1
    except Exception as e:
        logger.error("can't count result, count index is {0} ,count dsl is {1}".format(index_name, query_dsl))
    return -1

def es_multi_agg(index_name, query_dsl):
    try:
        res = es.search(index=index_name,
                        body=query_dsl,
                        request_timeout=100)
        return res["aggregations"]
    except Exception as e:
        logger.error("can't get info, query dsl is {}".format(query_dsl))
        return []


def es_agg(index_name, query_dsl):
    try:
        res = es_multi_agg(index_name,
                        query_dsl)
        result = []
        for agg_name in res:
            agg_result = res.get(agg_name)
            if agg_result.get("buckets", None):
                return agg_result.get("buckets", [])
            else:
                return agg_result
        return result
    except Exception as e:
        logger.error("can't get info, query dsl is {}".format(query_dsl))
        return []

