-- Initialize debezium user privileges to allow it work as a replicator
CREATE USER 'warehouse'@'localhost' IDENTIFIED BY 'testtest';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'warehouse';
FLUSH PRIVILEGES;

DROP DATABASE IF EXISTS `test_db`;
CREATE DATABASE `test_db`;
USE `test_db`;
