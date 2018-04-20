USE jdbcreader;

DROP TABLE IF EXISTS instance;
DROP TABLE IF EXISTS key_value_pairs;

CREATE TABLE instance (
	instance_name varchar(100) NOT NULL,
	hostname varchar(500) NULL,
	http_management_port bigint NULL,
	last_config_get_date timestamp NULL,
	CONSTRAINT pk_configuration_instance_instancename PRIMARY KEY (instance_name)
) ;

CREATE TABLE key_value_pairs (
	batch_name varchar(100) NOT NULL,
	connection_name varchar(100) NOT NULL,
	key_value varchar(100) NOT NULL,
	data_value varchar(1000) NOT NULL,
	CONSTRAINT pk_key_value_pairs PRIMARY KEY (batch_name, connection_name, key_value)
) ;
