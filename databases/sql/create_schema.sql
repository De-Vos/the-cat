CREATE SCHEMA felyx
    CREATE TABLE locations (
		id SERIAL NOT NULL, 
        wgs84_polygon VARCHAR(255), 
        title VARCHAR(255),
		PRIMARY KEY(id)
	)
    CREATE TABLE reservations (
		id SERIAL NOT NULL, 
        customer_id INT, 
        start_latitude DOUBLE PRECISION,
		start_longitude DOUBLE PRECISION,
		srid INT,
		net_price INT,
		location_id INT,
	    PRIMARY KEY(id)
	);


