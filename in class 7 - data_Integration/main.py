from datetime import datetime
import pandas as pd

county_df = pd.read_csv('acs2017_census_tract_data.csv')
covid_df = pd.read_csv('COVID_county_data.csv')

county_df.drop(columns=['TractId', 'Men', 'Women', 'Hispanic', 'White', 'Black', 'Native', 'Asian', 'Pacific', 'VotingAgeCitizen', 'Income', 'IncomeErr', 'ChildPoverty', 'Professional', 'Service', 'Office', 'Construction', 'Production', 'Drive', 'Carpool', 'Transit', 'Walk', 'OtherTransp', 'WorkAtHome', 'MeanCommute', 'Employed', 'PrivateWork', 'PublicWork', 'SelfEmployed', 'FamilyWork', 'Unemployment'], inplace=True)

county_state = []
county_summary = []
total_pop = []
total_poverty = []
per_capital_income = []

# checks for a unique state and county and append it to the list
for index, rows in county_df.iterrows():
    if "{}, {}".format(county_df['County'][index], county_df['State'][index]) not in county_state:
        county_state.append("{}, {}".format(county_df['County'][index], county_df['State'][index]))

#Groups the county and state, and gives the sum of total population, poverty, income per capital
county_summary = county_df.groupby(['County', 'State']).aggregate({'TotalPop': 'sum', 'Poverty': 'mean', 'IncomePerCap': 'sum'})
#appends the total pop/poverty/income per capital into a list
for summary in county_summary['TotalPop']:
    total_pop.append(summary)
for summary in county_summary['Poverty']:
    total_poverty.append(summary)
for summary in county_summary['IncomePerCap']:
    per_capital_income.append(summary)
# sorts the county_state. this is because the groupby method sorts it, so need to sort the county_state to match
county_state.sort()

# makes a new dataframe with all of these new changes
final_county_df = pd.DataFrame({'County': county_state, 'Population': total_pop, '% Poverty': total_poverty, 'PerCapitalIncome': per_capital_income})
# answers
print("answer 1" + str(final_county_df[final_county_df['County'] == 'Loudoun County, Virginia']))
print("answer 2" + str(final_county_df[final_county_df['County'] == 'Washington County, Oregon'])) 
print("answer 3" + str(final_county_df[final_county_df['County'] == 'Harlan County, Kentucky']))
print("answer 4" + str(final_county_df[final_county_df['County'] == 'Malheur County, Oregon']))
print("answer 5" + str(final_county_df[final_county_df['Population'] == final_county_df['Population'].max()]))
print("answer 6" + str(final_county_df[final_county_df['Population'] == final_county_df['Population'].min()]))

# Part B
covid_df.drop(columns=['fips'])

county_state.clear()

year = []
month = []
# checks for a unique state and county and append it to the list
for index, rows in covid_df.iterrows():
    # if "{}, {}".format(covid_df['county'][index], covid_df['state'][index]) not in county_state:
    county_state.append("{}, {}".format(covid_df['county'][index], covid_df['state'][index]))
    x = covid_df['date'][index].split('-')
    year.append(x[0])
    datetime_object = datetime.strptime(x[1], "%m")
    month_name = datetime_object.strftime("%B")
    month.append(month_name)

covid_df['year'] = year
covid_df['month'] = month
covid_df['county_state'] = county_state
print(covid_df.head(25))
covid_summary = covid_df.groupby(['county_state', 'month', 'year'])[['cases', 'deaths']].sum()
print(covid_summary['cases'][0])
# final_covid_df = pd.DataFrame({'County':county_state, 'month':month, 'year': year})

# for index, rows in covid_df.iterrows():
#     if covid_df['county'][index] not in county:
#         county.append(covid_df['county'][index])
#         county_state.append("{}, {}".format(covid_df['county'][index], covid_df['state'][index]))

#print(county_state)

# questions
total_deaths = 0
total_cases = 0
# index = 0
# for data in covid_df:
#     if data['county_state'][index] == "Malheur County, Oregon" and data['month'][index] == "February" and data['year'][index] == "2020":
#         total_deaths += data['deaths']
#         total_cases += data['cases']
#     index += 1
index = 0
total_cases = 0
total_deaths = 0
# print("Answer 1")
# print("{} cases {} deaths".format(total_cases, total_deaths))
# for data in covid_df:
#     if data['county_state'][index] == 'Malheur County, Oregon' and data['month'][index] == "August" and data['year'][index] == 2020:
#         total_deaths += data['deaths']
#         total_cases += data['cases']
#     index = index + 1
# index = 0
# print("Answer 2")
# print("{} cases {} deaths".format(total_cases, total_deaths))
# total_cases = 0
# total_deaths = 0
# for data in covid_df:
#     if data['county_state'][index] == 'Malheur County, Oregon' and data['month'][index] == "January" and data['year'][index] == 2021:
#         total_deaths += data['deaths']
#         total_cases += data['cases']
#     index = index + 1
# print("Answer 3")
# print("{} cases {} deaths".format(total_cases, total_deaths))
