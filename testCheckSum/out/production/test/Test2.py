import re

# replace all non-alphanumeric characters in a dict key with underscore(_)
def get_ansible_formatted_json_key(obj):
    for key in obj.keys():
        new_key = re.sub('[^0-9a-zA-Z-]', '_', key)
        if new_key != key:
            obj[new_key] = obj[key]
            del obj[key]
    return obj

