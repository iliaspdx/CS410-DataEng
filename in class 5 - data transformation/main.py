import pandas as pd
import numpy as np
import re

# Part A - Filter the data
def Filtering():
    return pd.read_csv('books.csv', usecols=["Identifier", "Place of Publication", "Date of Publication", "Publisher", "Title", "Author", "Contributors", "Flickr URL"])
# Part B - Tidying the data
    # "We performed data cleaning at the cell level, by accessing cell data as strings with the df[0].str attribute"
    # "Using the .str attribute on a dataframe like this allows you to apply any of the standard python string operations on the cells of the dataframe, w/o iterating over it"\
def replaceUncertainDates(df):
    df_final = df['Date of Publication'].str.extract(r'^(\d{4})', expand=False)
    df['Date of Publication'] = pd.to_numeric(df_final)
    #print(df.head(50))
    # df['Date of Publication'] = pd.to_numeric(df_final)
    # print(df['Date of Publication'].dtype)
    # print(df['Date of Publication'].head(50))
def onlyCity(df):
    # Transform all of the values in this column to be only the name of the city
    london = df['Place of Publication'].str.contains('London')
    oxford = df['Place of Publication'].str.contains('Oxford')
    plymouth = df['Place of Publication'].str.contains('Plymouth')
    # More 'bad' data but do that later or not
    df['Place of Publication'] = np.where(london, 'London', 
        np.where(oxford, 'Oxford', 
        np.where(plymouth, 'Plymouth', 
        df['Place of Publication'].str.replace('-', ' '))))

def applyMapFunc(item):
    if ' (' in item:
        return item[:item.find(' (')]
    elif '[' in item:
        return item[:item.find('[')]
    else:
        return item
def applyMapTutorial():
    uniPlaces = []
    with open('uniplaces.txt') as file:
        for line in file:
            if '[edit]' in line:
                state = line
            else:
                region = line[:line.find(' ')]
                uni = line[line.find('(') + 1:line.find(')')]
                uniPlaces.append((state, region, uni))
    #print(uniPlaces)
    df = pd.DataFrame(uniPlaces, columns=['State', 'City', 'University'])
    df = df.applymap(applyMapFunc)
    print(df.head(50))

"""
    # Removes any questionable dates --> [1900?]
    df_final = df['Date of Publication'].str.replace(r'\[\d{4}\?\]', str(np.nan), regex=True)
    # Removes any spaces
    df_final = df_final.str.replace(r'\s+', "", regex=True)
    #Strip [ 
    df_final = df_final.str.strip("[")
    df_final = df_final.str[ : 4]
    #df_final = df_final.str.replace(r'\d*\[\d{4}\]', "L", regex=True)
    # df_final = pd.to_numeric(df_final) """
    # print(df_final.head(50))


# df = Filtering()
# replaceUncertainDates(df)
# onlyCity(df)
# print(df['Date of Publication'].head(10))
# df_final = df['Date of Publication'].str.replace(r'\[\d{4}\?\]', 'abc', regex=True)
# print(df_final.head(50))
# df_final = replaceUncertainDates(df)
#df_final = df['Date of Publication'].str.strip(r'\[.*\]')
#df['Date of Publication'].str.strip('[')
#Regex expressions
"""
    wildcard which can match any single character by using the dot (.)
    * (star) means 0 or more --> \d*
    + (plus) means 1 or more --> \d+
"""
# print(df_final.head(50))
applyMapTutorial()


