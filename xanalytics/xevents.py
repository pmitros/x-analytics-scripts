'''
This is a set of helper functions for processing specifically
edX JSON events.
'''

import json
import numbers


from xanalytics.streaming import token, __select_field


def decode_event(data):
    ''' Convert browser events from string to JSON
    '''
    for line in data:
        if 'event' in line:
            try:
                while isinstance(line['event'], basestring):
                    line['event'] = json.loads(line['event'])
            except ValueError:
                line['event'] = 'Truncated'
            yield line


def desensitize_data(data, sensitive_fields, sensitive_event_fields):
    '''Remove known-sensitive fields and replace usernames with tokens.

    This does not fully deidentify data. It is helpful, however, for
    preventing a range of simple slip-ups in data handling.
    '''
    for line in data:
        for item in sensitive_fields:
            if item in line:
                del line[item]
            if 'context' in line and item in line['context']:
                del line['context'][item]
        for item in sensitive_event_fields:
            if 'event' in line and item in line["event"]:
                del line["event"]["item"]

        if 'username' in line:
            line['username'] = token(line['username'])
        yield line


def remove_redundant_data(data):
    '''Some versions of edX would put event data both at the top-level
    and in context. This cleans this up by removing it from context.
    '''
    for line in data:
        if 'context' in line:
            for item in list(line['context']):
                if item in line and line[item] == line['context'][item]:
                    del line['context'][item]
        for item in ['course_user_tags']:
            if 'context' in line and item in list(line['context']):
                del line['context'][item]
        if 'context' in line and len(line['context']) == 0:
            del line['context']
        yield line


def date_gt_filter(data, date):
    '''
    Filter data based on date. Date is a pretty free-form string
    format. If date is None, this is a no-op.
    '''
    if not date:
        for line in data:
            yield line

    date = dateutil.parser.parse(date)
    for line in data:
        if dateutil.parser.parse(line["time"]) > date:
            yield line


def filter_on_events(data, event_types):
    '''
    Filter data based on event types
    '''
    for d in data:
        if d['event_type'] in event_types:
            yield d


def filter_on_courses(data, courses):
    '''
    Filter data based on event types
    '''
    for d in data:
        if 'context' in d and \
           'course_id' in d['context'] and \
           d['context']['course_id'] in courses:
            yield d


def filter_on_fields(data, field_spec):
    '''
    Filter through fields

    field_spec maps field names to lists of possible values. For example:
    {'username':['jack','jill']}
    Will return all of the data where the user is either Jack or Jill
    '''
    for d in data:  # d is the event
        valid = True  # Does the event match the spec?
        for field in field_spec:  # Field we're looking at
            value = __select_field(d, field)
            if value not in field_spec[field]:
                valid = False
        if valid:
            yield d


def truncate_json(data, max_length):
    '''
    Truncate strings longer than max_length in an iterable of JSON
    objects. Long strings are replaced with 'none'

    >>> list(truncate_json([{'a': '12345', 'b': "123"}, \
                            {'c': ['1',2,'12345']}], 4))
    [{'a': None, 'b': '123'}, {'c': ['1', 2, None]}]
    '''
    for d in data:
        t = _truncate_json(d, max_length)
        yield t


def _truncate_json(data_item, max_length):
    '''
    Truncate strings longer than max_length in a JSON object. Long
    strings are replaced with 'none'

    >>> _truncate_json({'a': '12345', 'b': "123"}, 4)
    {'a': None, 'b': '123'}
    '''
    if isinstance(data_item, dict):
        for key in data_item:
            data_item[key] = _truncate_json(data_item[key], max_length)
        return data_item
    elif isinstance(data_item, numbers.Number):
        return data_item
    elif isinstance(data_item, basestring):
        if len(data_item) > max_length:
            return None
        return data_item
    elif isinstance(data_item, list):
        return list(_truncate_json(x, max_length) for x in data_item)
    elif data_item is None:
        return data_item
    else:
        error = "Could not truncate {repr} of type <{type}>"
        raise AttributeError(error.format(repr=repr(data_item),
                                          type=type(data_item)))

if __name__ == "__main__":
    import doctest
    doctest.testmod()
