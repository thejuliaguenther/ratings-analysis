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
    This function creates a dictionary mapping tuples containing the month and year of a timestamp 
    with the number of occurrences of that month and year 
    """
    split_lst = rating_dates.split(",")
    month_year_count = {}
        
    for i in xrange(len(split_lst)):
        curr_time = split_lst[i]
        time_digit_str = re.sub('[^0-9]','', curr_time)

        date_num =float(time_digit_str)
        new_date = datetime.fromtimestamp(date_num)
        new_date = new_date.strftime('%Y'+'-'+'%m')
        # date_year = new_date.year
        # date_month = new_date.month

        # date_string = str(date_month) + "-"+ str(date_year)

        # if date_string in month_year_count:
        #     month_year_count[date_string] += 1
        # else:
        #     month_year_count[date_string] = 1
        if new_date in month_year_count:
            month_year_count[new_date] += 1
        else:
            month_year_count[new_date] = 1
    key_value_list = get_keys_and_values(month_year_count)
    print key_value_list
    return key_value_list

def get_keys_and_values(count_dict):
    result = []
    for key in count_dict:
        pair = {'date':key, 'count':count_dict[key]}
        result.append(pair)
    return result 

