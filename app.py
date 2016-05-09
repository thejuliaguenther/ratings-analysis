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
    """
    This function creates a dictionary mapping each rating to 
    the number of times it appears
    """
    split_lst = movie_ratings.split(",")
    rating_breakdown_count = {'0.0':0, '0.5':0, '1.0':0, '1.5':0, '2.0':0, '2.5':0, '3.0':0, '3.5':0, '4.0':0, '4.5':0, '5.0':0}

    for i in xrange(len(split_lst)):
        curr_rating = split_lst[i]
        rating_digit_str = ''.join([x for x in curr_rating if x in '1234567890.'])

        # rating_num = float(rating_digit_str)

        # if curr_rating in rating_breakdown_count:
        #     rating_breakdown_count[rating_num] += 1
        # else:
        #     rating_breakdown_count[rating_num] = 1
        rating_breakdown_count[rating_digit_str] += 1
    key_value_list = get_keys_and_values(rating_breakdown_count,'rating','count')
    return key_value_list

def get_first_letter(string):
    split_str = string.split(" ")
    first_letter = ' '
    if split_str[0] == 'The' or split_str[0] == 'An' or split_str[0] == 'A':
        # If the first word is an article, use the second word when ordering words
        first_letter = split_str[1][0]
    elif not split_str[0][0].isalnum():
        i = 0
        while string[i].isalpha() and not string[i].isupper():
            i += 1

            if string[i].isupper():
                first_letter = string[i]
                break
            else:
                if len(split_str[0]) > 1:
                    first_letter = split_str[0][1]
    else:
      first_letter = split_str[0][0]

    return first_letter


