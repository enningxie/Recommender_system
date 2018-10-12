import pandas as pd
import os
import tqdm
import numpy as np
from collections import defaultdict
import argparse

"""
v2
"""
WIN_PATH = '/Users/ennin/Desktop/others/Recsys/datasets/ml-1m'

def argparser():
    args = argparse.ArgumentParser()
    args.add_argument('--raw_data_path', type=str,
                      default='/var/Data/xz/movielens-data/ml-1m/ratings.csv')
    args.add_argument('--train_data_path', type=str,
                      default='/var/Data/xz/movielens-data/ml-1m/ml_1m_train_03.csv')
    args.add_argument('--test_data_path', type=str,
                      default='/var/Data/xz/movielens-data/ml-1m/ml_1m_test_03.csv')
    args.add_argument('--negative_data_path', type=str,
                      default='/var/Data/xz/movielens-data/ml-1m/ml_1m_negative_03.csv')

    flags = args.parse_args()
    return flags


def process_raw_data(raw_path):
    raw_data = pd.read_csv(raw_path)

    # dict(old_id, new_id)
    dict_user = {}
    dict_item = {}
    counter_user = 0
    counter_item = 0
    for tmp_list in raw_data.values:
        tmp_user_id = tmp_list[0]
        tmp_item_id = tmp_list[1]
        if tmp_user_id not in dict_user.keys():
            dict_user[tmp_user_id] = counter_user
            counter_user += 1
        if tmp_item_id not in dict_item.keys():
            dict_item[tmp_item_id] = counter_item
            counter_item += 1

    # replace old_id with new_id
    raw_data['user_id'] = raw_data['user_id'].map(lambda a: dict_user[a])
    raw_data['item_id'] = raw_data['item_id'].map(lambda a: dict_item[a])

    return raw_data


def train_test_data(raw_path, train_path, test_path):
    print('train/test data start.')
    processed_data = process_raw_data(raw_path)
    # construct groups by user_id
    groups_data = processed_data.groupby(['user_id'])

    # generate train_data
    last_value_list = []
    # train_data = pd.DataFrame()
    # with tqdm.tqdm(total=len(groups_data)) as progress:
    #     for value, group in groups_data:
    #         tmp_group = group.sort_values(by='timestamp')
    #         tmp_index = tmp_group[-1:].index
    #         last_value_list.extend(tmp_index)
    #         tmp_group = tmp_group.drop(tmp_index)
    #         train_data = pd.concat([train_data, tmp_group])
    #         progress.update(1)

    with tqdm.tqdm(total=len(groups_data)) as progress:
        for value, group in groups_data:
            tmp_group = group.sort_values(by='timestamp')
            tmp_index = tmp_group[-1:].index
            last_value_list.extend(tmp_index)
            # tmp_group = tmp_group.drop(tmp_index)
            # train_data = pd.concat([train_data, tmp_group])
            progress.update(1)

    # generate test_data
    test_data = processed_data.iloc[last_value_list]

    train_data = processed_data.drop(last_value_list)

    train_data.to_csv(train_path)
    test_data.to_csv(test_path)
    print('train/test data finished.')


def negative_data_gen(test_path, raw_path, negative_path):
    print('negative data start.')
    # read test data
    tmp_test_data = pd.read_csv(test_path, index_col=0)
    # get processed data
    processed_data = process_raw_data(raw_path)

    negative_data = defaultdict(list)
    with tqdm.tqdm(total=len(tmp_test_data.values)) as progress:
        for value in tmp_test_data.values:
            tmp_tuple = (value[0], value[1])
            # tmp_count = 0
            # while tmp_count < 99:
            #     random_choice = np.random.choice(3705)
            #     if random_choice not in processed_data[processed_data['user_id'] == value[0]]['item_id']:
            #         negative_data[tmp_tuple].append(random_choice)
            #         tmp_count += 1

            #####
            tmp_set = set(range(3705))
            tmp_set = tmp_set - set(processed_data[processed_data['user_id'] == value[0]]['item_id'])
            random_choice = list(np.random.choice(list(tmp_set), size=99))
            negative_data[tmp_tuple].extend(random_choice)
            progress.update(1)

    negative_data_df = pd.DataFrame.from_dict(negative_data)
    negative_data_df.to_csv(negative_path)
    print('negative data finished.')


def main(flags):
    train_test_data(flags.raw_data_path, flags.train_data_path, flags.test_data_path)
    negative_data_gen(flags.test_data_path, flags.raw_data_path, flags.negative_data_path)


if __name__ == '__main__':
    FLAGS = argparser()
    main(FLAGS)