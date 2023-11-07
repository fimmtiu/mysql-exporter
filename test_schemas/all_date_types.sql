CREATE TABLE `all_date_types` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,

  `date_o` date DEFAULT NULL,
  `date_r` date NOT NULL DEFAULT '1904-06-16',
  `time_o` time DEFAULT NULL,
  `time_r` time NOT NULL DEFAULT '12:34:56',
  `datetime_o` datetime DEFAULT NULL,
  `datetime_r` datetime NOT NULL DEFAULT '1904-06-16 11:34:56',
  `timestamp_o` timestamp NULL DEFAULT NULL,
  `timestamp_r` timestamp NOT NULL DEFAULT '1996-01-22 11:34:56',

  PRIMARY KEY (`id`)
)
