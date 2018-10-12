import os

"""
v1
"""


DATA_PATH = './ratings.dat'
DATA_PATH_ = './ml_10m.rating'


def read_line():
    dict_user = {}
    dict_item = {}
    counter_user = 0
    counter_item = 0
    with open(DATA_PATH, 'r') as f:
        tmp_lines = f.readlines()
        for tmp_line in tmp_lines:
            tmp_list = tmp_line.split(sep='::')
            tmp_user_id = tmp_list[0]
            tmp_item_id = tmp_list[1]
            if tmp_user_id not in dict_user.keys():
                dict_user[tmp_user_id] = counter_user
                counter_user += 1
            if tmp_item_id not in dict_item.keys():
                dict_item[tmp_item_id] = counter_item
                counter_item += 1

    with open(DATA_PATH, 'r') as f_1:
        with open(DATA_PATH_, 'w') as f_2:
            tmp_lines_ = f_1.readlines()
            for tmp_line_ in tmp_lines_:
                tmp_list_ = tmp_line_.split(sep='::')
                tmp_user_id_ = tmp_list_[0]
                tmp_item_id_ = tmp_list_[1]
                concat_str = str(dict_user[tmp_user_id_]) + '\t' + str(dict_item[tmp_item_id_]) + '\t' + tmp_list_[2] + \
                             '\t' + tmp_list_[3]
                f_2.write(concat_str)

    print('done')


if __name__ == '__main__':
    read_line()