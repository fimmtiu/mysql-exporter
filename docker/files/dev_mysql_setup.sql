-- Initialize debezium user privileges to allow it work as a replicator
USE test_db;

CREATE USER 'warehouse'@'localhost' IDENTIFIED BY 'testtest';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'warehouse';
FLUSH PRIVILEGES;
