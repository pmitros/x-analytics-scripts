def canonical_course_tuple(course):
    '''
    Create a canonical course tuple.

    edX courses use many formats (usable keys, opaque keys, ad-hoc
    course run numbers, etc.). This cleans most of that up as of
    summer 2016 courses, and returns an org/course/run tuple.
    '''
    if course.startswith("course-v1:"):
        course = course[10:].replace('+', '/')
    if course.startswith("i4x://"):
        course = course[6:]
    course = course.split('/')
    course_run = course[2]
    course_run = course_run.replace('Q', 'T').replace("_", "").replace("-", "").replace("B", "")
    if course_run == 'X':
        course_run = "2016" 
    
    if course_run[1] == 'T':
        course_run = course_run.split('T')
        course_run.reverse()
        course_run = 'T'.join(course_run)
    if course_run[0] == 'T':
        course_run = course_run[2:] + course_run[:2]
    course[2] = course_run
    return course


def canonical_course_string(course):
    '''
    Create a course string in a standard format. For example:

    >>> canonical_course_string("course-v1:KyotoUx+002x+1T2016")
    'KyotoUx/002x/2016T1'
    >>> canonical_course_string("PekingX/02132750x/2015Q1")
    'PekingX/02132750x/2015T1'
    >>> canonical_course_string("i4x://PekingX/02132750x/2015Q1")
    'PekingX/02132750x/2015T1'
    '''
    return "/".join(canonical_course_tuple(course))

if __name__ == "__main__":
    import doctest
    doctest.testmod()
