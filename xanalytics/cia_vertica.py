'''
Import CIA World Factbook into an in-memory database.

Also:

* Extract and output country codes for all countries with per-capita GDP
  of less than 3000
'''

import click
import csv
import sys
import unicodedata
import vertica_python
import os

from cia_schema import float_columns, int_columns, dollar_columns, not_nations

conn = connection = vertica_python.connect(
    host=os.environ["WAREHOUSE_REMOTE_SERVER"],
    port=5433,
    user=os.environ["WAREHOUSE_USER"],
    password=os.environ["WAREHOUSE_PASSWORD"],
    database=os.environ["WAREHOUSE_DATABASE"]
)

c = conn.cursor()

##############
# Public API #
##############


def cursor():
    '''
    Return a cursor to the CIA World Factbook database. This is a
    sqlite in-memory database.
    '''
    global c
    return c


def available_fields():
    '''
    Return a list of columns in the database.
    '''
    return [x.split(' ')[0] for x in header.split(",")]

###########
# Private #
###########
header = None


def utf_8_encoder(unicode_csv_data):
    '''
    Stream processing operator going to UTF-8
    '''
    for line in unicode_csv_data:
        yield line.encode('utf-8')

try:
    import settings
    public_fs = settings.publicdatafs(compress=False)
    language_file = public_fs.open("languages.csv")
    cia_file = public_fs.open("cia-data-all.csv")
    data = csv.reader(utf_8_encoder(cia_file))
except:
    print "Could not use settings/x-analytics-fs. Falling back..."
    language_file = open("public_data/languages.csv")
    cia_file = open("public_data/cia-data-all.csv")
    data = csv.reader(cia_file)

languages = dict(x.split('\t')
                 for x
                 in language_file)

rownum = 0
memdata = []


def clean_header(s):
    '''
    Convert headers into something friendly for SQL, JSON, YAML,
    filenames, etc. by replacing all non-alphanumeric characters with
    underscores.
    '''
    s = s.strip()
    for i in range(len(s)):
        if not s[i].isalnum():
            s = s.replace(s[i], '_')
    for i in range(5):
        s = s.replace('__', '_')
    s = s.strip('_')
    return s


def identity_parser(c):
    '''
    nop. Return the string passed in.
    '''
    return c


def string_parser(c):
    '''
    Return escaped string
    '''
    return "'"+c.replace("'", "''")+"'"


def dollar_parser(c):
    '''
    Convert dollars to an int.
    '''
    c = c.replace('$', '')
    c = c.replace(' ', '')
    c = c.strip()
    if len(c) > 0:
        return str(int(c))
    else:
        return "NULL"


def float_parser(c):
    '''
    Convert a string to a float. Return `None` for the empty
    string.
    '''
    if len(c) > 0:
        return str(float(c))
    return "NULL"


def int_parser(c):
    '''
    Convert a string to an int. Return `None` for the empty
    string.
    '''
    if len(c) > 0:
        return str(int(c))
    return "NULL"

column_info = {}

for column in int_columns:
    column_info[column] = {'parser': int_parser,
                           'type': 'int'}
for column in float_columns:
    column_info[column] = {'parser': float_parser,
                           'type': 'float'}
for column in dollar_columns:
    column_info[column] = {'parser': dollar_parser,
                           'type': 'int'}


def headertype(c):
    if c in column_info:
        return column_info[c]['type']
    return "varchar(16384)"


def parsertype(c):
    '''
    Take the name of a column. Return a function which will parse that
    column into a native format. For example, given a column like
    population, it would return a function which would return a native
    python `int` given a string of the country's population.
    '''
    if c in column_info:
        return column_info[c]['parser']
    return string_parser

with click.progressbar(data) as bar:
    for row in bar:
        # Skip things which aren't really nations
        if row[0] in not_nations:
            continue
        
        if rownum == 0:
            row = [clean_header(r) for r in row]
            parsers = [parsertype(r) for r in row]
            fields = ",\n    ".join((c+" "+headertype(c) for c in row))+",language_list varchar(4096)"
            #fields = ",\n    ".join(["{name} {type}".format(name=name, type=vsql_type) for (name, vsql_type) in zip(input_tsvx.variables(), vsql_types)])
            c.execute('''DROP TABLE pmitros.cia;''')
            command = ''' CREATE TABLE pmitros.cia ({fields});'''.format(
                fields=fields
            )
            c.execute(command)
            commas = ",".join(["?"]*(len(row)+1))
            sql_template = 'INSERT INTO pmitros.cia VALUES ({values})'
        else:
            parsed_row = [p(r) for p, r in zip(parsers, row)]
            parsed_row.append(string_parser(languages[row[0]]))
            values = ",".join(parsed_row)
            sql_command = sql_template.format(values = values)
            sql_command = sql_command.decode('windows-1252')
            c.execute(sql_command)
        rownum = rownum + 1

conn.commit()
