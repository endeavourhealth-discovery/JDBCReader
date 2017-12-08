USE jdbcreader;

DROP TABLE IF EXISTS notification_message;
DROP TABLE IF EXISTS instance;
DROP TABLE IF EXISTS key_value_pairs;
DROP TABLE IF EXISTS batch_file;
DROP TABLE IF EXISTS batch;


CREATE TABLE batch (
	batch_id INT auto_increment,
	configuration_id varchar(100) NOT NULL,
	interface_type_name varchar(100) NOT NULL,
	batch_identifier varchar(500) NOT NULL,
	local_path varchar(1000) NULL,
	insert_date timestamp NOT NULL,
	complete boolean NULL DEFAULT false,
	complete_date timestamp NULL,
	notified boolean NULL DEFAULT false,
	notification_date timestamp NULL,
	CONSTRAINT pk_batch PRIMARY KEY (batch_id),
	CONSTRAINT uq_log_batch_configurationid_batchid UNIQUE (configuration_id,batch_id),
	CONSTRAINT uq_log_batch_configurationid_batchidentifier UNIQUE (configuration_id,batch_identifier)
);

CREATE TABLE batch_file (
	batch_file_id INT auto_increment,
	batch_id INT NOT NULL,
	file_type_identifier varchar(1000) NOT NULL,
	insert_date timestamp NOT NULL,
	filename varchar(1000) NOT NULL,
	downloaded boolean NULL DEFAULT false,
	download_date timestamp NULL,
	CONSTRAINT pk_batch_file PRIMARY KEY (batch_file_id),
	CONSTRAINT fk_batch_file_batch FOREIGN KEY (batch_id) REFERENCES batch (batch_id),
	CONSTRAINT uq_log_batchfile_batchid_filename UNIQUE (batch_id,filename),
	CONSTRAINT uq_log_batchfile_batchid_filetypeidentifier UNIQUE (batch_id,file_type_identifier)
) ;


CREATE TABLE notification_message (
	notification_message_id INT AUTO_INCREMENT,
	batch_id INT NOT NULL,
	configuration_id varchar(100) NOT NULL,
	message_uuid varchar(36) NOT NULL,
	notification_timestamp timestamp NOT NULL,
	outbound longtext NOT NULL,
	inbound longtext NULL,
	success boolean NOT NULL,
	error_text longtext NULL,
	CONSTRAINT pk_notification_message PRIMARY KEY (notification_message_id),
	CONSTRAINT uq_log_notificationmessage_messageuuid UNIQUE (message_uuid)
) ;

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
