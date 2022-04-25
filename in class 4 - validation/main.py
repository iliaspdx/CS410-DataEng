import pandas as pd

myVar = pd.read_csv('data.csv')

#Data validation - record type exists, no null values
# Checks if there is a null value in Record Type
    # Gets the entire Record Type Column
recordType = myVar['Record Type']
    #print the number of values of nulls. ANSW --> 0 This is valid
print(recordType.isnull().sum())

#Data validation - Every record has a record type from 1 to 3
    # get the size of the record type
    #ANSW - only displays a array from 1, 2, 3 which is expected behavior 
print(recordType.unique())

#Data validation - Every Data has NAN or nhs flag of 1
    #ANSW - FALSE some of the data set has a value of 0
NHSFlag = myVar['NHS Flag']
print(NHSFlag.unique())

#Data validation - Crashes occurred have approx the same longitude degrees (-122, to -121)
    #Answ - TRUE, all data set is either -122, -121 or NAN
LongitudeDegrees = myVar['Longitude Degrees']
print(LongitudeDegrees.unique())

#Data validation - All crashes occurred in the same year (2019)
    # Answ - TRUE, all data set is either 2019 or NAN
CrashYear = myVar['Crash Year']
print(CrashYear.unique())
#Data validation - Every data set has a Crash ID
    # ANSW - TRUE, all data set does contain a id, there is not NAN
CrashId = myVar['Crash ID']
CrashIdMiss = CrashId.isnull().sum()
if (CrashIdMiss>0):
    print("{} has {} missing value(s)".format('Crash ID', CrashIdMiss))
else:
    print("{} has NO missing value!".format('Crash ID'))

# Data validation - Every data set has a unique serial #
    # ANSW - FALSE, some data set may have duplicates 
SerialNumber = myVar['Serial #']
SerialNumberDropNa = SerialNumber.dropna()
print(SerialNumber.unique().size)
print("The size of all serial numbers in the data set is {} ", SerialNumberDropNa.size)
print("The size of all unique serial number is {}", SerialNumberDropNa.unique().size)