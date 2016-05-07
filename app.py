from sets import Set 
from datetime import *
import re

def remove_duplicate_tags(lst):
    """This function cleans the tags data from redis for each movie """

    split_lst = lst.split(",")
    seen = Set()
    result = []

    for i in xrange(len(split_lst)):
        tag = split_lst[i].strip()
        if i == 0:
            tag = tag[1:]
        if i == (len(split_lst)-1):
            tag = tag[:-1]
        tag = tag[1:-1]
        if tag not in seen:
            result.append(tag)
            seen.add(tag)

    return result

def process_timestamps(rating_dates):
    """
    This function creates a dictionary mapping the month and year of a timestamp 
    with the number of occurrences of that month and year 
    """
    split_lst = rating_dates.split(",")
    month_year_count = {}
        
    for i in xrange(len(split_lst)):
        curr_time = split_lst[i]
        time_digit_str = re.sub('[^0-9]','', curr_time)

        date_num =float(time_digit_str)
        new_date = datetime.fromtimestamp(date_num)
        new_date = new_date.strftime('%Y-%m')
        
        if new_date in month_year_count:
            month_year_count[new_date] += 1
        else:
            month_year_count[new_date] = 1
    key_value_list = get_keys_and_values(month_year_count,'date','count')
    sorted_list = sort_list(key_value_list)
    print sorted_list
    return sorted_list

def get_key(item):
    return item['date']

def sort_list(key_value_list):
    return sorted(key_value_list, key=get_key)

def get_keys_and_values(count_dict, key1, key2):
    result = []
    for key in count_dict:
        pair = {key1:key, key2:count_dict[key]}
        result.append(pair)
    return result 

def get_rating_breakdown(movie_ratings):
    split_lst = movie_ratings.split(",")
    rating_breakdown_count = {}

    for i in xrange(len(split_lst)):
        curr_rating = split_lst[i]
        rating_digit_str = ''.join([x for x in curr_rating if x in '1234567890.'])
        print rating_digit_str

        rating_num = float(rating_digit_str)

        if curr_rating in rating_breakdown_count:
            rating_breakdown_count[rating_num] += 1
        else:
            rating_breakdown_count[rating_num] = 1
    key_value_list = get_keys_and_values(rating_breakdown_count,'rating','count')
    print key_value_list
    return key_value_list


