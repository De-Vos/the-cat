SELECT  r.id, 
        r.customer_id, 
        r.start_latitude,
		r.start_longitude,
		r.srid,
		r.net_price,
		r.location_id,
		l.title as location_title
FROM felyx.reservations r
LEFT JOIN felyx.locations l
on r.location_id=l.id