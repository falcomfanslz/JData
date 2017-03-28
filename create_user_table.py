#-*- coding: utf-8 -*-

import pandas as pd
from collections import Counter

ACTION_201602_FILE = "data/JData_Action_201602.csv"
ACTION_201603_FILE = "data/JData_Action_201603.csv"
ACTION_201603_EXTRA_FILE = "data/JData_Action_201603_extra.csv"
ACTION_201604_FILE = "data/JData_Action_201604.csv"
COMMENT_FILE = "data/JData_Comment.csv"
PRODUCT_FILE = "data/JData_Product.csv"
USER_FILE = "data/JData_User.csv"
NEW_USER_FILE = "data/JData_User_New.csv"


df = pd.DataFrame(columns=["user_id", 'age', "sex",
                           "user_lv_cd", "browse_num",
                           "buy_num", "buy_browse_ratio",
                           "add_cart_num", "del_cart_num"])


def get_from_jdata_user():
    df_usr = pd.read_csv(NEW_USER_FILE, header=0)
    df_usr = df_usr[["user_id", "age", "sex", "user_lv_cd"]]
    return df_usr


def merge_behavior_count(group):
    group['browse_num'] = sum(group['browse_num'])
    group['addcart_num'] = sum(group['addcart_num'])
    group['delcart_num'] = sum(group['delcart_num'])
    group['buy_num'] = sum(group['buy_num'])
    group['favor_num'] = sum(group['favor_num'])
    group['click_num'] = sum(group['click_num'])

    if(group['browse_num'] == 0):
        group['buy_browse_ratio'] = 0.
    else:
        group['buy_browse_ratio'] = group[
            'buy_num'] / float(group['browse_num'])
    if(group['click_num']):
        group['buy_click_ratio'] = 0.
    else:
        group['buy_click_ratio'] = group['buy_num'] / float(group['click_num'])
    if(group['addcart_num'] == 0):
        group['buy_addcart_ratio'] = 0.
    else:
        group['buy_addcart_ratio'] = group[
            'buy_num'] / float(group['addcart_num'])

    return group


def add_type_count(group):
    behavior_type = group.type.astype(int)
    type_cnt = Counter(behavior_type)
    group['browse_num'] = type_cnt[1]
    group['addcart_num'] = type_cnt[2]
    group['delcart_num'] = type_cnt[3]
    group['buy_num'] = type_cnt[4]
    group['favor_num'] = type_cnt[5]
    group['click_num'] = type_cnt[6]

    return group[['user_id', 'browse_num', 'addcart_num',
                  'delcart_num', 'buy_num', 'favor_num',
                  'click_num']]


def get_from_action_data(fname, chunk_size=100000):
    # Number of Record: 18117303
    reader = pd.read_csv(fname, header=0, iterator=True)
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index=True)
    df_ac = df_ac.groupby(['user_id']).apply(add_type_count)

    return df_ac


def merge_action_data():
    df_ac = []
    df_ac.append(get_from_action_data(fname=ACTION_201602_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201603_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201603_EXTRA_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201604_FILE))

    df_ac = pd.concat(df_ac, ignore_index=True)
    df_ac = df_ac.groupby(['user_id']).apply(merge_behavior_count)


df_usr = get_from_jdata_user()
df[["user_id", "age", "sex", "user_lv_cd"]] = df_usr
merge_action_data()
