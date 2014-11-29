'''
WIP: Library for extracting information from events
'''

def source_institution(item):
    '''
    Figure out the source institution for an event. 

    For now, we raise an exception when we fail. In the future, we'll do something smarter. 

    This code is taken from a random one-off script. It needs to be cleaned up to be useful. 
    '''
    try:
        jitem = json.loads(item)
        if jitem['event_source'] == 'server': ## Middleware item
            evt_type = jitem['event_type']
            if '/courses/' in evt_type: ## Middleware item in course
                institution = evt_type.split('/')[2]
                return institution
            elif '/' in evt_type: ## Middle ware item not in course
                return "Global"
            else:  ## Specific server logging. One-off parser for each type
                #Survey of logs showed 4 event types: reset_problem
                #save_problem_check, save_problem_check_fail, save_problem_fail
                # All four of these have a problem_id, which we
                # extract from
                try: 
                    return jitem['event']['problem_id'].split('/')[2]
                except: 
                    return "Unhandled"
        elif jitem['event_source'] == 'browser': # Caught in browser
            page = jitem['page']
            if 'courses' in page:
                institution = page.split('/')[4]
                return institution
            else: 
                ## Code path unchecked/non-course has no 
                ## instrumentation
                return "BGE"
    except ValueError: ## TODO: 
        raise
    except: 
        raise


def date_string(item):
    '''
    Grab the data from an item.
    '''
    try: 
        return json.loads(item)['time'].split("T")[0]
    except: 
        return "XX"
