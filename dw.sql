-- create fact and dim table
CREATE TABLE dimvehicle (
    VehicleID                       VARCHAR PRIMARY KEY,
    BusInvolvement                 VARCHAR,
    HeavyRigidTruckInvolvement   VARCHAR,
    ArticulatedTruckInvolvement   VARCHAR
);

CREATE TABLE dimtime (
    TimeID     VARCHAR PRIMARY KEY,
    Hour       VARCHAR,
	TimeGroup  VARCHAR
);

CREATE TABLE dimspeed (
    SpeedID    VARCHAR PRIMARY KEY,
    SpeedZone  VARCHAR
);

CREATE TABLE dimpeople (
    PeopleID   VARCHAR PRIMARY KEY,
	AgeGroup  VARCHAR,
	Gender     VARCHAR
);

CREATE TABLE dimlocation (
    LocationID       VARCHAR PRIMARY KEY,
	State            VARCHAR,
	SA4Name          VARCHAR
);

CREATE TABLE dimlga (
    LGAID             VARCHAR PRIMARY KEY,
    LGAName           VARCHAR,
    LGASize           VARCHAR
);

CREATE TABLE dimevent (
    EventID              VARCHAR PRIMARY KEY,
    Christmas     VARCHAR,
    Easter       VARCHAR
);

CREATE TABLE dimdate (
    DateID      VARCHAR PRIMARY KEY,
    Year        INT,
    Month       INT,
    DayWeek     VARCHAR
);

CREATE TABLE dimcrash (
    CrashID           VARCHAR PRIMARY KEY,
    CrashType        VARCHAR,
    Severity          VARCHAR
);

CREATE TABLE fact (
    factid         VARCHAR PRIMARY KEY,
    crashid        VARCHAR,
    dateid         VARCHAR,
    locationid     VARCHAR,
    timeid         VARCHAR,
    vehicleid      VARCHAR,
    eventid        VARCHAR,
    peopleid       VARCHAR,
    lgaid          VARCHAR,
	speedid        VARCHAR,

    FOREIGN KEY (crashid)    REFERENCES dimcrash(crashid),
    FOREIGN KEY (dateid)     REFERENCES dimdate(dateid),
    FOREIGN KEY (locationid) REFERENCES dimlocation(locationid),
    FOREIGN KEY (timeid)     REFERENCES dimtime(timeid),
    FOREIGN KEY (vehicleid)  REFERENCES dimvehicle(vehicleid),
    FOREIGN KEY (eventid)    REFERENCES dimevent(eventid),
    FOREIGN KEY (peopleid)   REFERENCES dimpeople(peopleid),
    FOREIGN KEY (lgaid)      REFERENCES dimlga(lgaid),
	FOREIGN KEY (speedid)    REFERENCES dimspeed(speedid)
);

-- change to your own URL
COPY dimcrash FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimCrash.csv' WITH (FORMAT csv, HEADER true);
COPY dimdate FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimDate.csv' WITH (FORMAT csv, HEADER true);
COPY dimspeed FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimSpeed.csv' WITH (FORMAT csv, HEADER true);
COPY dimevent FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimEvent.csv' WITH (FORMAT csv, HEADER true);
COPY dimlga FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimLGA.csv' WITH (FORMAT csv, HEADER true);
COPY dimlocation FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimLocation.csv' WITH (FORMAT csv, HEADER true);
COPY dimpeople FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimPeople.csv' WITH (FORMAT csv, HEADER true);
COPY dimtime FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimTime.csv' WITH (FORMAT csv, HEADER true);
COPY dimvehicle FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/dimVehicle.csv' WITH (FORMAT csv, HEADER true);
COPY fact FROM 'D:/Andy/Study/UWA/Warehousing/datawarehousing/fact.csv' WITH (FORMAT csv, HEADER true);

-- Q1 --
SELECT 
    dp.RoadUser,
    COUNT(fc.factid) AS NumCrashes
FROM factcrashfatalities fc
JOIN dimpeople dp ON fc.peopleid = dp.peopleid
GROUP BY dp.RoadUser
ORDER BY NumCrashes DESC;

SELECT 
    dp.AgeGroup,
    COUNT(fc.factid) AS NumCrashes
FROM factcrashfatalities fc
JOIN dimpeople dp ON fc.peopleid = dp.peopleid
GROUP BY dp.AgeGroup
ORDER BY NumCrashes DESC;

-- Q2 --
SELECT 
    dl.State,
    COUNT(fc.factid) AS NumCrashes
FROM factcrashfatalities fc
JOIN dimlocation dl ON fc.locationid = dl.locationid
GROUP BY dl.State
ORDER BY NumCrashes DESC;

SELECT 
    dl.LGAName,
    COUNT(fc.factid) AS NumCrashes
FROM factcrashfatalities fc
JOIN dimlga dl ON fc.lgaid = dl.lgaid
GROUP BY dl.LGAName
ORDER BY NumCrashes DESC;

-- Q3 --
SELECT 
    EXTRACT(HOUR FROM dt.Time) AS HourOfDay,
    COUNT(fc.factid) AS NumCrashes
FROM factcrashfatalities fc
JOIN dimtime dt ON fc.timeid = dt.timeid
GROUP BY EXTRACT(HOUR FROM dt.Time)
ORDER BY HourOfDay;

-- Q4 --
SELECT 
    SUM(CASE WHEN dv.BusInvolvement = 'Yes' THEN 1 ELSE 0 END) AS BusCrashes,
    SUM(CASE WHEN dv.HeavyRigidTruckInvolvement = 'Yes' THEN 1 ELSE 0 END) AS HeavyTruckCrashes,
    SUM(CASE WHEN dv.ArticulatedTruckInvolvement = 'Yes' THEN 1 ELSE 0 END) AS ArticulatedTruckCrashes
FROM factcrashfatalities fc
JOIN dimvehicle dv ON fc.vehicleid = dv.vehicleid;

-- Q5 --
SELECT 
    SUM(CASE WHEN de.Christmas = 'Yes' THEN 1 ELSE 0 END) AS ChristmasCrashes,
    SUM(CASE WHEN de.Easter = 'Yes' THEN 1 ELSE 0 END) AS EasterCrashes,
    SUM(CASE WHEN de.FestivalOrNot = 'Yes' THEN 1 ELSE 0 END) AS FestivalCrashes
FROM factcrashfatalities fc
JOIN dimevent de ON fc.eventid = de.eventid;


-- Q6 --
SELECT 
    CASE 
        WHEN CAST(dr.SpeedZone AS INT) < 40 THEN 'Slow'
        WHEN CAST(dr.SpeedZone AS INT) BETWEEN 40 AND 80 THEN 'Medium'
        WHEN CAST(dr.SpeedZone AS INT) > 80 THEN 'Fast'
        ELSE 'Unknown'
    END AS SpeedCategory,
    COUNT(fc.factid) AS NumCrashes
FROM factcrashfatalities fc
JOIN dimroad dr ON fc.roadid = dr.roadid
WHERE dr.SpeedZone ~ '^[0-9]+$'
GROUP BY 
    CASE 
        WHEN CAST(dr.SpeedZone AS INT) < 40 THEN 'Slow'
        WHEN CAST(dr.SpeedZone AS INT) BETWEEN 40 AND 80 THEN 'Medium'
        WHEN CAST(dr.SpeedZone AS INT) > 80 THEN 'Fast'
        ELSE 'Unknown'
    END
ORDER BY NumCrashes DESC;
