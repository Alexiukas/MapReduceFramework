from mapreduce import MapReduce

CLICKS_DATASET = "data/clicks/"
USERS_DATASET = "data/users/"


# TASK  1 functions
def map_count(row, intermediate):
    intermediate[row[1]['date']].append(1)


def reduce_count(key, values):
    return {'date': key, 'count': sum(values)}


# TASK 2 functions
def map_filter(row, intermediate):
    if row[1]['country'] == 'LT':
        intermediate[row[1]['id']].append({'table': 'users', 'value': row[1]})


def map_transform(row, intermediate):
    intermediate[row[1]['user_id']].append({'table': 'clicks', 'value': row[1]})


def reduce_join(key, values):
    is_valid_user = False
    clicks = []

    for value in values:
        if value['table'] == 'users':
            is_valid_user = True
        else:
            clicks.append(value['value'].to_dict())

    if is_valid_user and len(clicks) != 0:
        return clicks


if __name__ == '__main__':
    # Task 1
    MapReduce({CLICKS_DATASET: map_count}, reduce_count, 'data/total_clicks.csv')

    # Task 2
    MapReduce({CLICKS_DATASET: map_transform, USERS_DATASET: map_filter}, reduce_join, 'data/filtered_clicks.csv')
