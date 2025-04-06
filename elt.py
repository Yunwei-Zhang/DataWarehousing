import pandas as pd
import calendar 
from datetime import datetime, timedelta

###
# 1. First step
# read data
# small changes
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
df_population_cleaned = df_population[['LGA code', 'Local Government Area', 'no..21', 'no..22']].copy()


# clean speed - change int to string(low, medium, fast)
df_fcrash_cleaned['Speed Limit'] = df_fcrash_cleaned['Speed Limit'].replace(-9, pd.NA)
df_fcrash_cleaned['Speed Limit'] = pd.to_numeric(df_fcrash_cleaned['Speed Limit'], errors='coerce')
df_fcrash_cleaned['Speed Limit'] = df_fcrash_cleaned['Speed Limit'].apply(
    lambda x: pd.NA if pd.isna(x)
    else 'Low' if x <= 40
    else 'Medium' if x <= 80
    else 'Fast'
)
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
# clean rows with missing data
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Bus Involvement'] != -9]
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Articulated Truck Involvement'] != -9]
df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Speed Limit'] != -9]
# df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['Heavy Rigid Truck Involvement'] != -9]
# df_fatelities_cleaned = df_fatelities_cleaned[df_fatelities_cleaned['National Remoteness Areas'] != 'Unknown']





### 
# 2. Second step
# Creating dim datasets
###

# dim date
df_date = df_fcrash_cleaned[["Year", "Month", "Dayweek", "Day of week"]].copy()
weekday_map = {
    'Monday': '1',
    'Tuesday': '2',
    'Wednesday': '3',
    'Thursday': '4',
    'Friday': '5',
    'Saturday': '6',
    'Sunday': '7'
}
df_date['WeekdayNum'] = df_date['Dayweek'].map(weekday_map)
df_date['DateID'] = 'Date' + df_date['Year'].astype(str) + df_date['Month'].astype(str).str.zfill(2) + df_date['WeekdayNum']
cols = ['DateID'] + [col for col in df_date.columns if col != 'DateID']
df_date = df_date.drop_duplicates()
df_date = df_date[cols]
df_date = df_date.drop_duplicates(subset='DateID', keep='first')
# dim time
df_time = df_fcrash_cleaned[["Time", "Time of Day"]].copy()
df_time = df_time.drop_duplicates()
df_time["TimeID"] = ['Time' + str(i) for i in range(1, len(df_time) + 1)]
cols = ['TimeID'] + [col for col in df_time.columns if col != 'TimeID']
df_vehicle = df_time[cols]
# dim location
df_location = df_fcrash_cleaned[["State", "National Remoteness Areas", "SA4 Name 2021", "National LGA Name 2021", "National Road Type"]].copy()
df_location = df_location.drop_duplicates(subset=['State', 'National Remoteness Areas', 'SA4 Name 2021'])
df_location['LocationID'] = ['Loc' + str(i) for i in range(1, len(df_location) + 1)]
cols = ['LocationID'] + [col for col in df_location.columns if col != 'LocationID']
df_location = df_location[cols]
# dim Vehicle Involvement
df_vehicle = df_fcrash_cleaned[['Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement']]
df_vehicle = df_vehicle.drop_duplicates()
df_vehicle["VehicleID"] = ['Veh' + str(i) for i in range(1, len(df_vehicle) + 1)]
cols = ['VehicleID'] + [col for col in df_vehicle.columns if col != 'VehicleID']
df_vehicle = df_vehicle[cols]
# dim Event
df_event = df_fcrash_cleaned[['Christmas Period', 'Easter Period', 'Festival or not']]
df_event = df_event.drop_duplicates()
df_event["EventID"] = ['Event' + str(i) for i in range(1, len(df_event) + 1)]
cols = ['EventID'] + [col for col in df_event.columns if col != 'EventID']
df_event = df_event[cols]
# dim people
df_people = df_fatelities_cleaned[['Age', 'Age Group', 'Gender', 'Road User']].copy()
df_people = df_people.drop_duplicates()
df_people["PeopleID"] = ['Peop' + str(i) for i in range(1, len(df_people) + 1)]
cols = ['PeopleID'] + [col for col in df_people.columns if col != 'PeopleID']
df_people = df_people[cols]
# dim crash
df_crash = df_fcrash_cleaned[['Crash ID', 'Crash Type', 'Speed Limit', 'Number Fatalities']].copy()
df_crash = df_crash.drop_duplicates()
df_crash.rename(columns={'Crash ID': 'CrashID'}, inplace=True)
# dim LGA
df_lga.rename(columns={df_lga.columns[0]: "National LGA Name 2021"}, inplace=True)
df_lga.rename(columns={df_lga.columns[1]: "Count of dwellings"}, inplace=True)
df_LGA = df_lga.copy()
df_population_cleaned.rename(columns={'Local Government Area': 'National LGA Name 2021'}, inplace=True)
df_LGA = df_LGA.merge(df_population_cleaned, 
    on=['National LGA Name 2021'],
    how='left'
)
df_LGA["LGAID"] = ['LGAID' + str(i) for i in range(1, len(df_LGA) + 1)]
cols = ['LGAID'] + [col for col in df_LGA.columns if col != 'LGAID']
df_LGA = df_LGA[cols]





###
# 3. Third step
# rename, drop and change order
###

# create fact table that remove all data into dim tables
df_fatelities_cleaned.rename(columns={'Crash ID': 'CrashID'}, inplace=True)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_crash, 
    on=['CrashID', 'Crash Type', 'Speed Limit'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_date, 
    on=["Year", "Month", "Dayweek", "Day of week"],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_location, 
    on=["State", "National Remoteness Areas", "SA4 Name 2021", "National LGA Name 2021", "National Road Type"],
    how='left'
)
df_fatelities_cleaned.rename(columns={'Time of day': 'Time of Day'}, inplace=True)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_time, 
    on=['Time', 'Time of Day'],
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
    on=['Age', 'Age Group', 'Gender', 'Road User'],
    how='left'
)
df_fatelities_cleaned = df_fatelities_cleaned.merge(df_LGA, 
    on=['National LGA Name 2021'],
    how='left'
)

# remove columns expect IDs
df_fatelities_cleaned.drop(columns=['Year', 'Month', 'Dayweek', 'Time', 'Day of week', 'Time of Day', 'State', 
'National Remoteness Areas', 'SA4 Name 2021', 'Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement', 'National Road Type',
'Christmas Period', 'Easter Period', 'Festival or not', 'WeekdayNum','Age', 'Age Group', 'Gender', 'Road User', 'Crash Type', 'Speed Limit', 'Number Fatalities',
'LGA code', 'no..21', 'no..22', 'National LGA Name 2021', 'Count of dwellings'], inplace=True)

# add FactID into first column
df_fatelities_cleaned["FactID"] = ['Fact' + str(i) for i in range(1, len(df_fatelities_cleaned) + 1)]
cols = ['FactID'] + [col for col in df_fatelities_cleaned.columns if col != 'FactID']
df_fatelities_cleaned = df_fatelities_cleaned[cols]

df_fcrash_fatelities_date_cleaned = df_fcrash_fatelities_date_cleaned.merge(
    df_date,
    on=['Year', 'Month', 'Dayweek', 'WeekdayNum'],
    how='left'
)
df_fcrash_fatelities_date_cleaned.drop(columns=['Year', 'Month', 'Dayweek', 'WeekdayNum', 'Day of week'], inplace=True)


# rename all columns and do final changes to suit SQL style
df_date.rename(columns={'Day of week': 'DayofWeek'}, inplace=True)
df_time.rename(columns={'Time of Day': 'TimeofDay'}, inplace=True)
df_location.rename(columns={'National Remoteness Areas': 'Area'}, inplace=True)
df_location.rename(columns={'SA4 Name 2021': 'SA4Name'}, inplace=True)
df_location.rename(columns={'National LGA Name 2021': 'LGAName'}, inplace=True)
df_location.rename(columns={'National Road Type': 'RoadType'}, inplace=True)
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
df_crash.rename(columns={'Number Fatalities': 'NumberFatalities'}, inplace=True)
df_LGA.rename(columns={'National LGA Name 2021': 'LGAName'}, inplace=True)
df_LGA.rename(columns={'Count of dwellings': 'Countofdwellings'}, inplace=True)
df_LGA.rename(columns={'LGA code': 'LGACode'}, inplace=True)
df_LGA.rename(columns={'no..21': 'Population2022'}, inplace=True)
df_LGA.rename(columns={'no..22': 'Population2023'}, inplace=True)

cols = ['TimeID'] + [col for col in df_time.columns if col != 'TimeID']
df_time = df_time[cols]


# use for testing
print(df_fatelities_cleaned)


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
