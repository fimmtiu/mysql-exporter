CREATE TABLE `all_number_types` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,

  `tinyint_so` tinyint(4) DEFAULT NULL,
  `tinyint_sr` tinyint(4) NOT NULL,
  `tinyint_uo` tinyint(4) unsigned DEFAULT NULL,
  `tinyint_ur` tinyint(4) unsigned NOT NULL,
  `smallint_so` smallint(6) DEFAULT NULL,
  `smallint_sr` smallint(6) NOT NULL,
  `smallint_uo` smallint(6) unsigned DEFAULT NULL,
  `smallint_ur` smallint(6) unsigned NOT NULL,
  `mediumint_so` mediumint(9) DEFAULT NULL,
  `mediumint_sr` mediumint(9) NOT NULL,
  `mediumint_uo` mediumint(9) unsigned DEFAULT NULL,
  `mediumint_ur` mediumint(9) unsigned NOT NULL,
  `int_so` int(11) DEFAULT NULL,
  `int_sr` int(11) NOT NULL,
  `int_uo` int(11) unsigned DEFAULT NULL,
  `int_ur` int(11) unsigned NOT NULL,
  `bigint_so` bigint(20) DEFAULT NULL,
  `bigint_sr` bigint(20) NOT NULL,
  `bigint_uo` bigint(20) unsigned DEFAULT NULL,
  `bigint_ur` bigint(20) unsigned NOT NULL,

  `float_o` float DEFAULT NULL,
  `float_r` float NOT NULL,
  `double_o` double DEFAULT NULL,
  `double_r` double NOT NULL,

  `smalldecimal_o` decimal(6,3) DEFAULT NULL,
  `smalldecimal_r` decimal(6,3) NOT NULL,
  `mediumdecimal_o` decimal(12,6) DEFAULT NULL,
  `mediumdecimal_r` decimal(12,6) NOT NULL,
  `bigdecimal_o` decimal(24,12) DEFAULT NULL,
  `bigdecimal_r` decimal(24,12) NOT NULL,

  PRIMARY KEY (`id`)
)
