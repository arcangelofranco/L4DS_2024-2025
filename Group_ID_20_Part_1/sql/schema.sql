CREATE TABLE crash(
    crash_id NVARCHAR(50) PRIMARY KEY,
    rd_no NCHAR(8) UNIQUE NOT NULL,
    crash_date DATETIME NOT NULL,
    posted_speed_limit INT NOT NULL,
    traffic_control_device NVARCHAR(50) NOT NULL,
    device_condition  NVARCHAR(50) NOT NULL,
    weather_condition NVARCHAR(50) NOT NULL,
    lighting_condition NVARCHAR(50) NOT NULL,
    first_crash_type NVARCHAR(50) NOT NULL,
    trafficway_type NVARCHAR(50) NOT NULL,
    alignment NVARCHAR(50) NOT NULL,
    roadway_surface_cond NVARCHAR(50) NOT NULL,
    road_defect NVARCHAR(50) NOT NULL,
    report_type NVARCHAR(50) NOT NULL,
    crash_type NVARCHAR(50) NOT NULL,
    prim_contributory_cause NVARCHAR(100) NOT NULL,
    sec_contributory_cause NVARCHAR(100) NOT NULL,
);

CREATE TABLE date(
    date_id NVARCHAR(50) PRIMARY KEY,
    crash_time TIME NOT NULL,
    crash_period NCHAR(2) NOT NULL,
    crash_day INT NOT NULL,
    crash_month INT NOT NULL,
    crash_year INT NOT NULL,
    crash_day_of_week NVARCHAR(10) NOT NULL,
    crash_season NVARCHAR(6) NOT NULL,
    date_police_notified DATETIME NOT NULL
);

CREATE TABLE location(
    location_id NVARCHAR(50) PRIMARY KEY,
    street_no INT NOT NULL,
    street_direction NCHAR(1) NOT NULL,
    street_name NVARCHAR(100) NOT NULL,
    beat_of_occurrence INT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    location_point NCHAR(12) NOT NULL
);

CREATE TABLE injury(
    injury_id NVARCHAR(50) PRIMARY KEY,
    most_severe_injury NVARCHAR(50) NOT NULL,
    injuries_total INT NOT NULL,
    injuries_fatal INT NOT NULL,
    injuries_incapacitating INT NOT NULL,
    injuries_non_incapacitating INT NOT NULL,
    injuries_reported_not_evident INT NOT NULL,
    injuries_no_indication INT NOT NULL
);

CREATE TABLE person(
    person_id NVARCHAR(50) PRIMARY KEY,
    person_type NVARCHAR(50) NOT NULL,
    crash_date DATETIME NOT NULL,
    city NVARCHAR(100) NOT NULL,
    state NCHAR(2) NOT NULL,
    sex NCHAR(1) NOT NULL,
    age INT NOT NULL,
    safety_equipment NVARCHAR(50) NOT NULL,
    airbag_deployed NVARCHAR(50) NOT NULL,
    ejection NVARCHAR(50) NOT NULL,
    injury_classification NVARCHAR(50) NOT NULL,
    driver_action NVARCHAR(50) NOT NULL,
    driver_vision NVARCHAR(50) NOT NULL,
    physical_condition NVARCHAR(50) NOT NULL,
    bac_result NVARCHAR(50) NOT NULL,
    damage_category NVARCHAR(20) NOT NULL
);

CREATE TABLE vehicle(
    vehicle_id NVARCHAR(50) PRIMARY KEY,
    crash_date DATETIME NOT NULL,
    unit_no INT NOT NULL,
    unit_type NVARCHAR(50) NOT NULL,
    make NVARCHAR(150) NOT NULL,
    model NVARCHAR(150) NOT NULL,
    lic_plate_state NCHAR(2) NOT NULL,
    vehicle_year INT NOT NULL,
    vehicle_defect NVARCHAR(50) NOT NULL,
    vehicle_type NVARCHAR(50) NOT NULL,
    vehicle_use NVARCHAR(50) NOT NULL,
    travel_direction NVARCHAR(10) NOT NULL,
    maneuver NVARCHAR(50) NOT NULL,
    occupant_cnt INT NOT NULL,
    first_contact_point NVARCHAR(50) NOT NULL
);

CREATE TABLE damage(
    damage_cost FLOAT NOT NULL,
    num_units INT NOT NULL,
    crash_id NVARCHAR(50) NOT NULL,
    date_id NVARCHAR(50) NOT NULL,
    location_id NVARCHAR(50) NOT NULL,
    injury_id NVARCHAR(50) NOT NULL,
    person_id NVARCHAR(50) NOT NULL,
    vehicle_id NVARCHAR(50) NOT NULL,
    FOREIGN KEY(crash_id)
        REFERENCES crash(crash_id) ON UPDATE CASCADE on DELETE CASCADE,
    FOREIGN KEY(date_id)
        REFERENCES date(date_id) ON UPDATE CASCADE on DELETE CASCADE,
    FOREIGN KEY(location_id)
        REFERENCES location(location_id) ON UPDATE CASCADE on DELETE CASCADE,
    FOREIGN KEY(injury_id)
        REFERENCES injury(injury_id) ON UPDATE CASCADE on DELETE CASCADE,
    FOREIGN KEY(person_id)
        REFERENCES person(person_id) ON UPDATE CASCADE on DELETE CASCADE,
    FOREIGN KEY(vehicle_id)
        REFERENCES vehicle(vehicle_id) ON UPDATE CASCADE on DELETE CASCADE
);