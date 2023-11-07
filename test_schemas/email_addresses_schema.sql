CREATE TABLE `email_addresses` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(19) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,
  `contact_id` bigint(20) unsigned DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `import_id` bigint(20) unsigned DEFAULT NULL,
  `default_email` tinyint(1) DEFAULT NULL,
  `account_id` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_email_addresses_on_contact_id_and_default_email` (`contact_id`,`default_email`),
  KEY `index_email_addresses_on_account_id` (`account_id`),
  KEY `index_email_addresses_on_address_and_account_id` (`address`(191),`account_id`)
) ENGINE=InnoDB AUTO_INCREMENT=347515 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci
