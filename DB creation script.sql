-- Create the database
CREATE DATABASE CabData;
GO

USE CabData;
GO

-- Create staging table for raw data loading
CREATE TABLE Staging_CabTrips (
    tpep_pickup_datetime datetime2,
    tpep_dropoff_datetime datetime2,
    passenger_count int,
    trip_distance decimal(10, 2),
    store_and_fwd_flag varchar(3),
    PULocationID int,
    DOLocationID int,
    fare_amount decimal(10, 2),
    tip_amount decimal(10, 2)
);
GO

-- Create final table with constraints and indexes
CREATE TABLE CabTrips (
    tpep_pickup_datetime datetime2,
    tpep_dropoff_datetime datetime2,
    passenger_count int,
    trip_distance decimal(10, 2),
    store_and_fwd_flag varchar(3),
    PULocationID int,
    DOLocationID int,
    fare_amount decimal(10, 2),
    tip_amount decimal(10, 2),
    CONSTRAINT UQ_CabTrips_Unique UNIQUE (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count)
);
GO

-- Add indexes for query optimization
CREATE INDEX IX_CabTrips_PULocationID ON CabTrips (PULocationID);
CREATE INDEX IX_CabTrips_TripDistance ON CabTrips (trip_distance);
GO