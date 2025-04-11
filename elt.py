import pandas as pd
import calendar 
from datetime import datetime, timedelta

###
# Data Warehousing 2025
# Project 1 (April 10)
# Author: Yunwei Zhang
###

###
# 1. First step
# read data, copy data and do basic cleaning operations
# df_fcrash: data of 'bitre_fatal_crashes_dec2024.xlsx/BITRE_Fatal_Crash'
# df_fcrash_date: data of 'bitre_fatal_crashes_dec2024.xlsx/BITRE_Fatal_Crash_Count_By_Date'
# df_fatelities: data of 'bitre_fatalities_dec2024.xlsx/BITRE_Fatality'
# df_fatelities_date: data of 'bitre_fatalities_dec2024.xlsx/BITRE_Fatality_Count_By_Date'
# df_lga： data of 'LGA (count of dwellings).csv'
# df_population: data of 'Population estimates by LGA, Significant Urban Area, Remoteness Area, Commonwealth Electoral Division and State Electoral Division, 2001 to 2023.xlsx'
###

# read data
df_fcrash = pd.read_excel('bitre_fatal_crashes_dec2024.xlsx', sheet_name='BITRE_Fatal_Crash', skiprows=4)
df_fcrash_date = pd.read_excel('bitre_fatal_crashes_dec2024.xlsx', sheet_name='BITRE_Fatal_Crash_Count_By_Date', skiprows=2)
df_fatelities = pd.read_excel('bitre_fatalities_dec2024.xlsx', sheet_name='BITRE_Fatality', skiprows=4)
df_fatelities_date = pd.read_excel('bitre_fatalities_dec2024.xlsx', sheet_name='BITRE_Fatality_Count_By_Date', skiprows=2)
df_lga = pd.read_csv('LGA (count of dwellings).csv', skiprows=11, header=None)
df_lga.drop(df_lga.columns[2], axis=1, inplace=True)
df_population = pd.read_excel('Population estimates by LGA, Significant Urban Area, Remoteness Area, Commonwealth Electoral Division and State Electoral Division, 2001 to 2023.xlsx', sheet_name='Table 1', skiprows=6)
df_population = df_population[:-2]
df_lga = df_lga[:-9]

# copy data
df_fcrash_cleaned = df_fcrash.copy()
df_fatelities_cleaned = df_fatelities.copy()
df_fcrash_fatelities_date_cleaned = df_fcrash_date.copy()
df_population_cleaned = df_population[['LGA code', 'Local Government Area', 'no..22']].copy()

# clean LGA
map_LGA_number = dict(zip(df_lga[0], df_lga[1]))
df_fcrash_cleaned['Count of dwellings'] = df_fcrash_cleaned['National LGA Name 2021'].map(map_LGA_number)

# clean festivals - add new column that know whether today has event
df_fcrash_cleaned['Festival or not'] = df_fcrash_cleaned.apply(
    lambda row: 'Yes' if row['Christmas Period'] == 'Yes' or row['Easter Period'] == 'Yes' else 'No',
    axis=1
)

# clean day of week (change string to int)
df_fcrash_fatelities_date_cleaned['Number Fatalities'] = df_fatelities_date['Number Fatalities']
df_fcrash_fatelities_date_cleaned.rename(columns={'Day Of Week': 'Dayweek'}, inplace=True)
weekday_map = {
    'Monday': '1',
    'Tuesday': '2',
    'Wednesday': '3',
    'Thursday': '4',
    'Friday': '5',
    'Saturday': '6',
    'Sunday': '7'
}
df_fcrash_fatelities_date_cleaned['WeekdayNum'] = df_fcrash_fatelities_date_cleaned['Dayweek'].map(weekday_map)
month_map = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}
df_fcrash_fatelities_date_cleaned['Month'] = df_fcrash_fatelities_date_cleaned['Month'].map(month_map)


### 
# 2. Second step
# Creating dim datasets
###

# ========== DIM DATE ==========
df_date = df_fcrash_cleaned[["Year", "Month", "Dayweek"]].copy()

# Optional: weekday mapping if needed later
weekday_map = {
    'Monday': '1',
    'Tuesday': '2',
    'Wednesday': '3',
    'Thursday': '4',
    'Friday': '5',
    'Saturday': '6',
    'Sunday': '7'
}

df_date = df_date.drop_duplicates().reset_index(drop=True)
df_date["DateID"] = range(1, len(df_date) + 1)
df_date = df_date[["DateID", "Year", "Month", "Dayweek"]]

# ========== DIM TIME ==========
df_time = df_fatelities_cleaned[['Time']].drop_duplicates().copy()
df_time["Hour"] = pd.to_datetime(df_time["Time"], format='%H:%M:%S', errors='coerce').dt.hour
df_time = df_time.dropna(subset=["Hour"])
def assign_time_group(hour):
    if 0 <= hour < 6:
        return "Early Morning"
    elif 6 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 18:
        return "Afternoon"
    elif 18 <= hour < 21:
        return "Evening"
    else:
        return "Night"

df_time["TimeGroup"] = df_time["Hour"].apply(assign_time_group)
df_time = df_time.reset_index(drop=True)
df_time["TimeID"] = pd.Series(range(1, len(df_time) + 1), dtype="Int64")
df_time = df_time[["TimeID", "Time", "Hour", "TimeGroup"]]

# ========== DIM LOCATION ==========
df_location = df_fcrash_cleaned[["State", "SA4 Name 2021"]].drop_duplicates().reset_index(drop=True)
df_location["LocationID"] = range(1, len(df_location) + 1)
df_location = df_location[["LocationID", "State", "SA4 Name 2021"]]

# ========== DIM VEHICLE ==========
df_vehicle = df_fcrash_cleaned[['Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement']]
df_vehicle = df_vehicle.drop_duplicates().reset_index(drop=True)
df_vehicle["VehicleID"] = range(1, len(df_vehicle) + 1)
df_vehicle = df_vehicle[["VehicleID"] + [col for col in df_vehicle.columns if col != 'VehicleID']]

# ========== DIM EVENT ==========
df_event = df_fcrash_cleaned[['Christmas Period', 'Easter Period']].drop_duplicates().reset_index(drop=True)
df_event["EventID"] = range(1, len(df_event) + 1)
df_event = df_event[["EventID"] + [col for col in df_event.columns if col != 'EventID']]

# ========== DIM PEOPLE ==========
df_people = df_fatelities_cleaned[['Age Group', 'Gender']].drop_duplicates().reset_index(drop=True)
df_people["PeopleID"] = range(1, len(df_people) + 1)
df_people = df_people[["PeopleID", "Age Group", "Gender"]]

# ========== DIM CRASH ==========
df_crash = df_fcrash_cleaned[['Crash ID', 'Crash Type', 'Number Fatalities']].drop_duplicates().reset_index(drop=True)
df_crash.rename(columns={'Crash ID': 'CrashID'}, inplace=True)


# ========== DIM LGA ==========
df_lga.rename(columns={df_lga.columns[0]: "National LGA Name 2021"}, inplace=True)
df_lga.rename(columns={df_lga.columns[1]: "Count of dwellings"}, inplace=True)
df_LGA = df_lga.copy()
df_LGA["LGAID"] = pd.Series(range(1, len(df_LGA) + 1), dtype="Int64")
df_LGA = df_LGA[["LGAID", "National LGA Name 2021", "Count of dwellings"]]

# ========== DIM ROAD ==========
df_road = df_fcrash_cleaned[['Speed Limit']].drop_duplicates().reset_index(drop=True)
df_road["SpeedID"] = range(1, len(df_road) + 1)
df_road = df_road[["SpeedID", "Speed Limit"]]



###
# 3. Third step
# rename, drop and change order
###

# create fact table that remove all data into dim tables
df_fatelities_cleaned.rename(columns={'Crash ID': 'CrashID'}, inplace=True)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_crash, 
    on=['CrashID', 'Crash Type'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_date, 
    on=["Year", "Month", "Dayweek"],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_location, 
    on=["State", "SA4 Name 2021"],
    how='left'
)
df_fatelities_cleaned.rename(columns={'Time of day': 'Time of Day'}, inplace=True)
df_fatelities_cleaned['Time'] = pd.to_datetime(df_fatelities_cleaned['Time'], format='%H:%M:%S', errors='coerce').dt.time
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_time, 
    on=['Time'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_vehicle, 
    on=['Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_event, 
    on=['Christmas Period', 'Easter Period'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_people, 
    on=['Age Group', 'Gender'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_LGA, 
    on=['National LGA Name 2021'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_road, 
    on=['Speed Limit'],
    how='left'
)

# clean rows with missing data
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Speed Limit'] != -9]
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Age'] != -9]
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Age Group'] != -9]
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Road User'] != 'Unknown']
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['SA4 Name 2021'] != 'Unknown']

# remove columns expect IDs
df_fatelities_cleaned.drop(columns=['Year', 'Month', 'Dayweek', 'Time of Day', 'State', 
'National Remoteness Areas', 'SA4 Name 2021', 'Bus Involvement','Time', 'Hour', 'TimeGroup', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement',
'Christmas Period', 'Easter Period','Age', 'Day of week', 'Age Group', 'Gender', 'Crash Type', 'Number Fatalities', 
'National LGA Name 2021', 'Count of dwellings', 'Speed Limit', 'National Road Type', 'Road User'], inplace=True)

# add FactID into first column
df_fatelities_cleaned["FactID"] = ['Fact' + str(i) for i in range(1, len(df_fatelities_cleaned) + 1)]
cols = ['FactID'] + [col for col in df_fatelities_cleaned.columns if col != 'FactID']
df_fatelities_cleaned = df_fatelities_cleaned[cols]

df_fcrash_fatelities_date_cleaned = df_fcrash_fatelities_date_cleaned.merge(
    df_date,
    on=['Year', 'Month', 'Dayweek'],
    how='left'
)
df_fcrash_fatelities_date_cleaned.drop(columns=['Year', 'Month', 'Dayweek'], inplace=True)


# rename all columns and do final changes to suit SQL style
df_date.rename(columns={'Day of week': 'DayofWeek'}, inplace=True)
df_location.rename(columns={'National Remoteness Areas': 'RemotenessArea'}, inplace=True)
df_location.rename(columns={'SA4 Name 2021': 'SA4Name'}, inplace=True)
df_vehicle.rename(columns={'Bus Involvement': 'BusInvolvement'}, inplace=True)
df_vehicle.rename(columns={'Heavy Rigid Truck Involvement': 'HeavyRigidTruckInvolvement'}, inplace=True)
df_vehicle.rename(columns={'Articulated Truck Involvement': 'ArticulatedTruckInvolvement'}, inplace=True)
df_event.rename(columns={'Christmas Period': 'Christmas'}, inplace=True)
df_event.rename(columns={'Easter Period': 'Easter'}, inplace=True)
df_event.rename(columns={'Festival or not': 'FestivalOrNot'}, inplace=True)
df_people.rename(columns={'Age Group': 'AgeGroup'}, inplace=True)
df_people.rename(columns={'Road User': 'RoadUser'}, inplace=True)
df_crash.rename(columns={'Crash Type': 'CrashType'}, inplace=True)
df_crash.rename(columns={'Speed Limit': 'SpeedLimit'}, inplace=True)
df_crash.rename(columns={'Number Fatalities': 'Severity'}, inplace=True)
df_LGA.rename(columns={'National LGA Name 2021': 'LGAName'}, inplace=True)
df_LGA.rename(columns={'Count of dwellings': 'LGASize'}, inplace=True)
df_LGA.rename(columns={'LGA code': 'LGACode'}, inplace=True)
df_LGA.rename(columns={'no..22': 'Population'}, inplace=True)
df_road.rename(columns={'Speed Limit': 'SpeedZone'}, inplace=True)
df_road.rename(columns={'National Road Type': 'RoadType'}, inplace=True)

# small changes to dim time 
cols = ['TimeID'] + [col for col in df_time.columns if col != 'TimeID']
df_time = df_time[cols]
df_time = df_time.drop(columns=['Time'])

# use for testing
print(df_fatelities_cleaned)
print(df_location)

### 
# 4. Last step
# output
# df_date, df_time, df_location, df_vehicle, df_event, df_people, df_crash, df_LGA, df_fatelities_cleaned
###

df_date.to_csv('dimDate.csv', index=False)
df_time.to_csv('dimTime.csv', index=False)
df_location.to_csv('dimLocation.csv', index=False)
df_vehicle.to_csv('dimVehicle.csv', index=False)
df_event.to_csv('dimEvent.csv', index=False)
df_people.to_csv('dimPeople.csv', index=False)
df_crash.to_csv('dimCrash.csv', index=False)
df_LGA.to_csv('dimLGA.csv', index=False)
df_fatelities_cleaned.to_csv('fact.csv', index=False)
df_road.to_csv('dimSpeed.csv', index=False)
