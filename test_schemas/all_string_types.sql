CREATE TABLE `all_string_types` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,

  `char_o` char(8) DEFAULT NULL,
  `char_r` char(8) NOT NULL,
  `varchar_o` varchar(12) DEFAULT NULL,
  `varchar_r` varchar(12) DEFAULT NULL,
  `text_o` text DEFAULT NULL,
  `text_r` text NOT NULL,
  `mediumtext_o` mediumtext DEFAULT NULL,
  `mediumtext_r` mediumtext NOT NULL,
  `longtext_o` longtext DEFAULT NULL,
  `longtext_r` longtext NOT NULL,

  `binary_o` binary(8) DEFAULT NULL,
  `binary_r` binary(8) NOT NULL,
  `varbinary_o` varbinary(12) DEFAULT NULL,
  `varbinary_r` varbinary(12) DEFAULT NULL,
  `blob_o` blob DEFAULT NULL,
  `blob_r` blob NOT NULL,
  `mediumblob_o` mediumblob DEFAULT NULL,
  `mediumblob_r` mediumblob NOT NULL,
  `longblob_o` longblob DEFAULT NULL,
  `longblob_r` longblob NOT NULL,

  PRIMARY KEY (`id`)
)
