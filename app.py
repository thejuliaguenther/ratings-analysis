from sets import Set 

def remove_duplicate_tags(lst):
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