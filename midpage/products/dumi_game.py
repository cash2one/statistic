# coding=utf-8
import os

from midpage import base

source = 'qianxun'


def average(results):
    count = 0
    sum_value = 0
    for values in results.values():
        for value in values:
            value = float(value)
            sum_value += value
            count += 1
    if count == 0:
        return 0
    return float(sum_value) / count


def map_results(results):
    ret = {}
    for result in results:
        if ret.get(result['baiduid']) is None:
            ret[result['baiduid']] = []
        ret[result['baiduid']].append(result['timestamp'])
    return ret


def group_values_time(values):
    values.sort()
    ret = []
    start_time = None
    end_time = None
    i = 1
    for value in values:
        if start_time is None:
            # print "group%s:%s" % (i, value)
            start_time = value
        elif (value - start_time) > 1800:
            if end_time is not None:
                ret.append(end_time - start_time)
            else:
                ret.append(0)
            start_time = value
            end_time = None
            i += 1
            # print "group%s:%s" % (i, value)
        else:
            # print "group%s:%s" % (i, value)
            end_time = value
    if start_time and end_time:
        ret.append(end_time - start_time)
    else:
        ret.append(0)
    return ret


def group_values_rounds(values):
    values.sort()
    ret = []
    start_time = None
    end_time = None
    count = 0
    for value in values:
        count += 1
        if start_time is None:
            start_time = value
        elif (value - start_time) > 1800:
            ret.append(count - 1)
            count = 1
            start_time = value
            end_time = None
        else:
            end_time = value
    ret.append(count)
    return ret


def average_interaction_time(results):
    results = map_results(results)
    for key, values in results.items():
        # print "key:%s" % key
        results[key] = group_values_time(values)
    return average(results)


def average_interaction_rounds(results):
    results = map_results(results)
    for key, values in results.items():
        results[key] = group_values_rounds(values)
    return average(results)


class Product(base.CRMMidpageProduct):
    default_query = {
        'source': 'qianxun',
    }

    index_map = {  # example，具体类需要复写
        u'PV': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': 'pv',
            },
            'type': 'count',
        },
        u'UV': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': 'pv',
            },
            'distinct_key': 'baiduid',
            'type': 'distinct_count',
        },
        u'游戏点击数': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': 'a_click_game_item'
            },
            'type': 'count',
        },
        u'人均点击数': {
            'numerator': u'游戏点击数',
            'denominator': u'UV',
            'type': 'percent',
        },
        u'展现中间页数': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': 'pv',
            },
            'distinct_key': 'referr_query.sid',
            'type': 'distinct_count',
        },
        u'session平均交互时间': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': {
                    '$exists': True,
                }
            },
            'fields': {
                'timestamp': 1,
                'baiduid': 1,
            },
            'local': average_interaction_time,
            'type': 'output',
        },
        u'session平均交互轮数': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': {
                    '$exists': True,
                },
            },
            'fields': {
                'timestamp': 1,
                'baiduid': 1,
            },
            'local': average_interaction_rounds,
            'type': 'output',
        },
        u'页面平均停留时间': {
            'query': {
                'query.cat': 'dumi_game',
                'query.act': 'stay_time',
                'query.duration': {'$lt': 1800},
            },
            'field': 'query.duration',
            'type': 'avg',
        },
    }

    index_group = [{
        'name': 'total',
        'query': {}
    }, {
        'name': 'ios',
        'query': {'os': 'ios'}
    }, {
        'name': 'android',
        'query': {'os': 'android'}
    }]

    file_group = [{
        'name': 'total',
        'query': {}
    }, {
        'name': 'NA',
        'query': {'client': 'NA'}
    },{
        'name': 'MB',
        'query': {'client': 'MB'}
    }]

    index_order = [
        u'PV',
        u'UV',
        u'游戏点击数',
        u'人均点击数',
        u'展现中间页数',
        u'session平均交互时间',
        u'session平均交互轮数',
        u'页面平均停留时间',
    ]