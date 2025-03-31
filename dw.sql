-- create fact and dim table
CREATE TABLE dimvehicle (
    VehicleID                      VARCHAR PRIMARY KEY,
    BusInvolvement                 VARCHAR,
    HeavyRigidTruckInvolvement    VARCHAR,
    ArticulatedTruckInvolvement   VARCHAR
);

CREATE TABLE dimtime (
    TimeID     VARCHAR PRIMARY KEY,
    Time       TIME,
    TimeOfDay  VARCHAR
);

CREATE TABLE dimpeople (
    PeopleID   VARCHAR PRIMARY KEY,
    Age        INT,
    AgeGroup   VARCHAR,
    Gender     VARCHAR,
    RoadUser   VARCHAR
);

CREATE TABLE dimlocation (
    LocationID  VARCHAR PRIMARY KEY,
    State       VARCHAR,
    Area        VARCHAR,
    SA4Name     VARCHAR,
    LGAName     VARCHAR,
    RoadType    VARCHAR
);

CREATE TABLE dimlga (
    LGAID             VARCHAR PRIMARY KEY,
    LGAName           VARCHAR,
    CountOfDwellings  INT,
    LGACode           VARCHAR,
    Population2022    FLOAT,
    Population2023    FLOAT
);

CREATE TABLE dimevent (
    EventID        VARCHAR PRIMARY KEY,
    Christmas      VARCHAR,
    Easter         VARCHAR,
    FestivalOrNot  VARCHAR
);

CREATE TABLE dimdate (
    DateID      VARCHAR PRIMARY KEY,
    Year        INT,
    Month       INT,
    DayWeek     VARCHAR,
    DayOfWeek   VARCHAR,
    WeekdayNum  INT
);

CREATE TABLE dimcrash (
    CrashID           VARCHAR PRIMARY KEY,
    CrashType         VARCHAR,
    SpeedLimit        VARCHAR,
    NumberFatalities  INT
);

CREATE TABLE factcrashfatalities (
    factid         VARCHAR PRIMARY KEY,
    crashid        VARCHAR,
    dateid         VARCHAR,
    locationid     VARCHAR,
    timeid         VARCHAR,
    vehicleid      VARCHAR,
    eventid        VARCHAR,
    peopleid       VARCHAR,
    lgaid          VARCHAR,

    FOREIGN KEY (crashid)    REFERENCES dimcrash(crashid),
    FOREIGN KEY (dateid)     REFERENCES dimdate(dateid),
    FOREIGN KEY (locationid) REFERENCES dimlocation(locationid),
    FOREIGN KEY (timeid)     REFERENCES dimtime(timeid),
    FOREIGN KEY (vehicleid)  REFERENCES dimvehicle(vehicleid),
    FOREIGN KEY (eventid)    REFERENCES dimevent(eventid),
    FOREIGN KEY (peopleid)   REFERENCES dimpeople(peopleid),
    FOREIGN KEY (lgaid)      REFERENCES dimlga(lgaid)
);

-- change to your own URL
COPY dimcrash FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimCrash.csv' WITH (FORMAT csv, HEADER true);
COPY dimdate FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimDate.csv' WITH (FORMAT csv, HEADER true);
COPY dimevent FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimEvent.csv' WITH (FORMAT csv, HEADER true);
COPY dimlga FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimLGA.csv' WITH (FORMAT csv, HEADER true);
COPY dimlocation FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimLocation.csv' WITH (FORMAT csv, HEADER true);
COPY dimpeople FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimPeople.csv' WITH (FORMAT csv, HEADER true);
COPY dimtime FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimTime.csv' WITH (FORMAT csv, HEADER true);
COPY dimvehicle FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimVehicle.csv' WITH (FORMAT csv, HEADER true);
COPY factcrashfatalities FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/fact.csv' WITH (FORMAT csv, HEADER true);
