from utils.es_utils import get_all_data
from utils.global_constant import config
import pandas as pd


def get_recent_message():
    '''
    训练数据准备样例
    '''
    query_dsl = {
        "_source": [
            "cust_id",
            "news_id",
            "tag_list"
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "event_time": {
                                "gt": "2022-04-26 06:05:27"
                            }
                        }
                    },
                    {
                        "term": {
                            "event_type": 2
                        }
                    },
                    {
                        "exists": {"field": "tag_list"}
                    }
                ]
            }
        }
    }
    search_result = get_all_data(config.get("es_index", "news_click_index"), query_dsl)
    click_message = []
    user_ids = set()
    batch_num = 1000
    is_first = True
    colum_order = ["news_id","tag_list","cust_id","hold_stock_list","optional_stock"]
    for each in search_result:
        if each.get("_source"):
            each = each.get("_source")
            tag_list = []
            if each.get("tag_list"):
                tag_list = each.get("tag_list")
            each["tag_list"] = ",".join(tag_list)
            click_message.append(each)
            user_ids.add(each.get("cust_id"))
        if len(click_message)>batch_num:
            click_data = pd.DataFrame(click_message)
            user_dict = list()
            user_query_dsl = {
                "query": {
                    "bool": {
                        "must": {
                            "terms": {
                                "cust_id": list(user_ids)
                            }
                        }
                    }
                }
            }
            user_profile_message = get_all_data(config.get("es_index", "cust_profile_index"), user_query_dsl)
            for each in user_profile_message:
                each = each.get("_source")

                for key in ("hold_stock_list","optional_stock"):
                    host_stock = []
                    if each.get(key):
                        host_stock = [stock["stock_name"] for stock in each.get(key) if stock["stock_name"]]
                    each[key]=",".join(host_stock)

                user_dict.append(each)
            user_data=pd.DataFrame(user_dict)
            print("begin to merge")
            merge_result = pd.merge(click_data,user_data,how="inner",on='cust_id')[colum_order]
            #信息合并
            print("begin to save")
            if is_first:
                is_first = False
                merge_result.to_csv("data/test.csv",sep='\t',index=False,mode='w')
            else:
                merge_result.to_csv("data/test.csv", sep='\t',index=False,mode='a',header=False)
            click_message.clear()
            user_ids.clear()

get_recent_message()


