# create mirror table
CREATE TABLE mirror (
	event_id INT PRIMARY KEY,
	event_datetime TIMESTAMP NOT NULL,
	item_bibnum INT,
	item_title VARCHAR (255),
	item_callnum VARCHAR (32),
	item_type VARCHAR (32),
	item_collection VARCHAR (32),
	item_subjects VARCHAR (255)
);

# sample insertion
INSERT INTO mirror (
	event_id,
	event_datetime,
	item_bibnum,
	item_title,
	item_callnum,
	item_type,
	item_collection,
	item_subjects)
VALUES (
	201903261944000010070884381,
	'03/26/2019 07:44:00 PM',
	2604131,
	'Taking the leap freeing ourselves from old habits and fears',
	'294.3444 C4518T 2009',
	'acbk',
	'nanf',
	'Spiritual life Buddhism');
