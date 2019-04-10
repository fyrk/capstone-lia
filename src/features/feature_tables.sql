-- checkouts table definition
CREATE TABLE checkouts (
    event_id VARCHAR (32) PRIMARY KEY,
    event_timestamp TIMESTAMP,
    bibnum BIGINT,
    callnum VARCHAR (64)
);

-- catalog table definition
CREATE TABLE catalog (
	bibnumber BIGINT PRIMARY KEY,
	f6mo_checkouts INT,
    first_checkout TIMESTAMP,
	mediatype VARCHAR (16),
	collection VARCHAR (16),
	title TEXT,
	subjects TEXT
);

-- Checkouts table select as
CREATE TABLE IF NOT EXISTS checkouts
AS SELECT
    event_id,
    event_datetime,
    item_bibnum,
    item_callnum
FROM mirror;

-- Catalog table select as
CREATE TABLE IF NOT EXISTS catalog (
AS SELECT
	item_bibnum,
	COUNT(item_bibnum) AS first_6mo_checkouts,
	MIN(event_datetime) AS first_checkout,
	item_type,
	item_collection,
	item_title,
	item_subjects
FROM (
	SELECT
		item_bibnum,
		event_datetime,
		item_type,
		item_collection,
		item_title,
		item_subjects
	FROM mirror
	GROUP BY
		item_bibnum,
		event_datetime,
		event_datetime,
		item_type,
		item_collection,
		item_title,
		item_subjects
	HAVING
		event_datetime < MIN(event_datetime) + '6 months'::INTERVAL
) AS smf
GROUP BY
	item_bibnum,
	item_type,
	item_collection,
	item_title,
	item_subjects
;
