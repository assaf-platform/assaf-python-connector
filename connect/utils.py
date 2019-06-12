def normalize_names(*name):
    return ".".join([n.replace("-", "") for n in name])


def take_first_from_lists(dict):
    new_dict = {}
    for k, v in dict.items():
        if len(v) == 1:
            new_dict[k] = v[0]
        else:
            new_dict[k] = v

    return new_dict