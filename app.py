from sets import Set 
from datetime import *
import re

def remove_duplicate_tags(lst):
    """This method cleans the tags data from redis for each movie """

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

    # A dictionary mapping tuples containing the month and year of a timestamp 
    # with the number of occurrences of that month and year 
    split_lst = rating_dates.split(",")
    month_year_count = {}

    print split_lst
        
    for i in xrange(len(split_lst)):
        curr_time = split_lst[i]
        time_digit_str = re.sub('[^0-9]','', curr_time)
        # if i == 0:
        #     time = split_lst[i].strip()
        #     time  = time [2:-1]
        # if i == (len(split_lst)-1):
        #     time = split_lst[i].strip()
        #     time = time[1:-2]
        # else:
        #     time = split_lst[i].strip()
        #     time = time[1:-1]

        date_num =float(time_digit_str)
        new_date = datetime.fromtimestamp(date_num)
        print new_date
    #     date_year = new_date.year
    #     date_month = new_date.month

    #     if (date_month,date_year) in month_year_count:
    #         month_year_count[(date_month,date_year)] += 1
    #     else:
    #         month_year_count[(date_month,date_year)] = 1
    # print month_year_count
