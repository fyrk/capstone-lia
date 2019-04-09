DROP TABLE IF EXISTS mirror;

CREATE TABLE mirror (
	event_id VARCHAR (32) PRIMARY KEY,
	event_datetime TIMESTAMP,
	item_bibnum BIGINT,
	item_title TEXT,
	item_callnum VARCHAR (64),
	item_type VARCHAR (16),
	item_collection VARCHAR (16),
	item_subjects TEXT
);
