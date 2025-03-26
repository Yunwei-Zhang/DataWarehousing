import pandas as pd
import calendar 
from datetime import datetime, timedelta

###
# First step
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

# copy data
df_fcrash_cleaned = df_fcrash.copy()
df_fatelities_cleaned = df_fatelities[["Crash ID", "Gender", "Age", "Age Group"]].copy()
df_fcrash_fatelities_date_cleaned = df_fcrash_date.copy()

# clean speed
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
# clean festivals
df_fcrash_cleaned['Festival or not'] = df_fcrash_cleaned.apply(
    lambda row: 'Yes' if row['Christmas Period'] == 'Yes' or row['Easter Period'] == 'Yes' else 'No',
    axis=1
)

# combine date together
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
# Second step
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
# dim time
df_time = df_fcrash_cleaned[["Time", "Time of Day"]].copy()
df_time = df_time.drop_duplicates()
df_time["TimeID"] = ['Time' + str(i) for i in range(1, len(df_time) + 1)]
cols = ['TimeID'] + [col for col in df_time.columns if col != 'TimeID']
df_vehicle = df_time[cols]
# dim location
df_location = df_fcrash_cleaned[["State", "National Remoteness Areas", "SA4 Name 2021", "National LGA Name 2021", "Count of dwellings", "National Road Type"]].copy()
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
df_people = df_fatelities_cleaned.copy()
df_people["PeopleID"] = ['Peop' + str(i) for i in range(1, len(df_people) + 1)]
cols = ['PeopleID'] + [col for col in df_people.columns if col != 'PeopleID']
df_people = df_people[cols]





###
# Third step
#  rename, drop and change order
###

df_fcrash_cleaned.insert(1, 'DateID', df_date['DateID'])
df_fcrash_cleaned.insert(2, 'LocationID', df_location['LocationID'])
df_fcrash_cleaned = df_fcrash_cleaned.merge(df_time, 
    on=['Time', 'Time of Day'],
    how='left'
)
df_fcrash_cleaned = df_fcrash_cleaned.merge(df_vehicle, 
    on=['Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement'],
    how='left'
)
df_fcrash_cleaned = df_fcrash_cleaned.merge(df_event, 
    on=['Christmas Period', 'Easter Period', 'Festival or not'],
    how='left'
)
df_people_grouped = df_people.groupby('Crash ID').agg({
    'PeopleID': lambda x: ', '.join(str(i) for i in x),
    'Gender': lambda x: ', '.join(str(i) for i in x),
    'Age Group': lambda x: ', '.join(str(i) for i in x)
}).reset_index()
df_fcrash_cleaned = df_fcrash_cleaned.merge(df_people_grouped, on='Crash ID', how='left')
df_fcrash_cleaned.drop(columns=['Year', 'Month', 'Dayweek', 'Time', 'Day of week', 'Time of Day', 'State', 
'National Remoteness Areas', 'SA4 Name 2021', 'Count of dwellings', 'Bus Involvement', 'Heavy Rigid Truck Involvement', 'Articulated Truck Involvement', 'National Road Type',
'Christmas Period', 'Easter Period', 'Festival or not', 'Gender', 'Age Group'], inplace=True)


df_fcrash_fatelities_date_cleaned = df_fcrash_fatelities_date_cleaned.merge(
    df_date,
    on=['Year', 'Month', 'Dayweek', 'WeekdayNum'],
    how='left'
)
df_fcrash_fatelities_date_cleaned.drop(columns=['Year', 'Month', 'Dayweek', 'WeekdayNum', 'Day of week'], inplace=True)


#print(df_date.head(10))
#print(df_fcrash_fatelities_date_cleaned.head(10))


### 
# Last step
# output
# df_fcrash_cleaned, df_date, df_time, df_location, df_vehicle, df_event, df_people, df_fcrash_fatelities_date_cleaned
###

df_fcrash_cleaned.to_csv('dimCrashes.csv', index=False)
df_date.to_csv('dimDate.csv', index=False)
df_time.to_csv('dimTime.csv', index=False)
df_location.to_csv('dimLocation.csv', index=False)
df_vehicle.to_csv('dimVehicle.csv', index=False)
df_event.to_csv('dimEvent.csv', index=False)
df_people.to_csv('dimPeople.csv', index=False)
df_fcrash_fatelities_date_cleaned.to_csv('dimDateCount.csv', index=False)
