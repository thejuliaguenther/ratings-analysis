from sets import Set 

def remove_duplicate_tags(lst):
    split_lst = lst.split(",")
    seen = Set()
    result = []

    for i in xrange(len(split_lst)):
        tag = split_lst[i].strip()
        if tag not in seen:
            result.append(tag)
            seen.add(tag)

    return result