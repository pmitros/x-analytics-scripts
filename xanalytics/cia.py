'''
Import CIA World Factbook into an in-memory database.

Also:
* Extract and output country codes for all countries with per-capita GDP
  of less than 3000
'''

import csv
import sys
import sqlite3
import unicodedata
import settings

from cia_schema import float_columns, int_columns, dollar_columns, not_nations

conn = sqlite3.connect(":memory:")
conn.text_factory = str
c = conn.cursor()

##############
# Public API #
##############


def cursor():
    '''
    Return a cursor to the CIA World Factbook database
    '''
    global c
    return c

def available_fields():
    return [x.split(' ')[0] for x in header.split(",")]

###########
# Private #
###########
header = None


def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


languages = dict(x.split('\t')
                 for x
                 in settings.publicdatafs(compress=False).open("languages.csv"))
data = csv.reader(utf_8_encoder(settings.publicdatafs(compress=False).open("cia-data-all.csv")))
rownum = 0
memdata = []


def clean_header(s):
    s = s.strip()
    for i in range(len(s)):
        if not s[i].isalnum():
            s = s.replace(s[i], '_')
    for i in range(5):
        s = s.replace('__', '_')
    s = s.strip('_')
    return s


def identity_parser(c):
    return c


def dollar_parser(c):
    c = c.replace('$', '')
    c = c.replace(' ', '')
    c = c.strip()
    if len(c) > 0:
        return int(c)
    else:
        return None


def float_parser(c):
    if len(c)>0:
        return float(c)
    return None


def int_parser(c):
    if len(c)>0:
        return int(c)
    return None

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
    return "text"


def parsertype(c):
    if c in column_info:
        return column_info[c]['parser']
    return identity_parser

for row in data:
    # Skip things which aren't really nations
    if row[0] in not_nations:
        continue

    if rownum == 0:
        global header
        row = [clean_header(r) for r in row]
        parsers = [parsertype(r) for r in row]
        header = ",".join((c+" "+headertype(c) for c in row))+",language_list"
        command = ''' CREATE TABLE cia ({header}) '''.format(header=header)
        c.execute(command)
        commas = ",".join(["?"]*(len(row)+1))
        insert_command = 'INSERT INTO cia VALUES ({commas})'.format(commas=commas)
    else:
        parsed_row = [p(r) for p,r in zip(parsers, row)]
        parsed_row.append(languages[row[0]])
        c.executemany(insert_command, [parsed_row])
    rownum = rownum + 1

if __name__ == '__main__':
    # Test case: extract countries with per-capita GDP under $3000
    countries = []

    for row in c.execute('SELECT Internet_country_code, Languages from cia where GDP_per_capita_PPP < 3000'):
        cc = row[0]
        if len(cc) < 1:
            continue
        countries.append(cc[1:3])

    print countries == ['st', 'sh', 'kh', 'et', 'cm', 'bf', 'gh', 'lr', 'tz', 'cd', 'ye', 'pk', 'ps', 'in', 'ls', 'ke', 'tj', 'af', 'bd', 'er', 'sb', 'ps', 'rw', 'so', 'la', 'mw', 'bj', 'eh', 'tg', 'tl', 'cf', 'vn', 'ml', 'td', 'tk', 'sn', 'mz', 'ug', 'ne', 'gn', 'fm', 'ng', 'tv', 'mh', 'ht', 'sl', 'md', 'gw', 'gm', 'uz', 'bi', 'ni', 'mg', 'sd', 'np', 'kp', 'zm', 'pg', 'ci', 'zw', 'mr', 'kg', 'mm', 'km']


# Fields: ['id', 'Labor_force', 'Labor_force_details', 'Judicial_branch', 'Land_use', 'Land_use_details', 'Land_boundaries', 'Land_boundaries_details', 'Infant_mortality_rate', 'Infant_mortality_rate_details', 'Industries', 'Waterways', 'Waterways_details', 'Inflation_rate_consumer_prices', 'Inflation_rate_consumer_prices_details', 'Languages', 'Languages_details', 'Internet_country_code', 'HIV_AIDS_adult_prevalence_rate', 'HIV_AIDS_adult_prevalence_rate_details', 'HIV_AIDS_people_living_with_HIV_AIDS', 'HIV_AIDS_people_living_with_HIV_AIDS_details', 'HIV_AIDS_deaths', 'HIV_AIDS_deaths_details', 'Telephones_main_lines_in_use', 'Telephones_main_lines_in_use_details', 'Sex_ratio', 'Sex_ratio_details', 'Internet_users', 'Internet_users_details', 'Television_broadcast_stations', 'Television_broadcast_stations_details', 'Geographic_coordinates', 'Age_structure', 'Age_structure_details', 'Radio_broadcast_stations', 'Radio_broadcast_stations_details', 'GDP_composition_by_sector', 'GDP_composition_by_sector_details', 'Disputes_international', 'Area', 'Area_details', 'Irrigated_land', 'Irrigated_land_details', 'Map_references', 'Location', 'Country_name', 'Coastline', 'Coastline_details', 'Imports_partners', 'Imports_partners_details', 'Constitution', 'Diplomatic_representation_in_the_US', 'Manpower_reaching_militarily_significant_age_annually', 'Manpower_reaching_militarily_significant_age_annually_details', 'Distribution_of_family_income_Gini_index', 'Distribution_of_family_income_Gini_index_details', 'Oil_production', 'Oil_production_details', 'Debt_external', 'Debt_external_details', 'Exports', 'Exports_details', 'Oil_exports', 'Oil_exports_details', 'Median_age', 'Median_age_details', 'Oil_consumption', 'Oil_consumption_details', 'Oil_imports', 'Oil_imports_details', 'Oil_proved_reserves', 'Oil_proved_reserves_details', 'Natural_gas_proved_reserves', 'Natural_gas_proved_reserves_details', 'Executive_branch', 'Exchange_rates', 'Exchange_rates_details', 'Ethnic_groups', 'Ethnic_groups_details', 'Market_value_of_publicly_traded_shares', 'Market_value_of_publicly_traded_shares_details', 'Exports_partners', 'Exports_partners_details', 'Airports', 'Airports_details', 'School_life_expectancy_primary_to_tertiary_education', 'School_life_expectancy_primary_to_tertiary_education_details', 'Population_below_poverty_line', 'Population_below_poverty_line_details', 'Household_income_or_consumption_by_percentage_share', 'Household_income_or_consumption_by_percentage_share_details', 'Electricity_exports', 'Electricity_exports_details', 'Electricity_consumption', 'Electricity_consumption_details', 'Capital', 'Background', 'Labor_force_by_occupation', 'Labor_force_by_occupation_details', 'Exports_commodities', 'Commercial_bank_prime_lending_rate', 'Commercial_bank_prime_lending_rate_details', 'Death_rate', 'Death_rate_details', 'Pipelines', 'Pipelines_details', 'GDP_official_exchange_rate', 'GDP_official_exchange_rate_details', 'Stock_of_direct_foreign_investment_at_home', 'Stock_of_direct_foreign_investment_at_home_details', 'Stock_of_direct_foreign_investment_abroad', 'Stock_of_direct_foreign_investment_abroad_details', 'Administrative_divisions', 'Total_renewable_water_resources', 'Total_renewable_water_resources_details', 'Freshwater_withdrawal_domestic_industrial_agricultural', 'Freshwater_withdrawal_domestic_industrial_agricultural_details', 'Agriculture_products', 'Military_branches', 'Birth_rate', 'Birth_rate_details', 'Education_expenditures', 'Education_expenditures_details', 'Budget', 'Budget_details', 'Climate', 'Imports_commodities', 'Political_parties_and_leaders', 'Population', 'Population_details', 'Nationality', 'Natural_resources', 'Net_migration_rate', 'Net_migration_rate_details', 'Geography_note', 'Political_pressure_groups_and_leaders', 'Economy_overview', 'Telephones_mobile_cellular', 'Telephones_mobile_cellular_details', 'Independence', 'Natural_gas_exports', 'Natural_gas_exports_details', 'Natural_gas_imports', 'Natural_gas_imports_details', 'Natural_gas_con
