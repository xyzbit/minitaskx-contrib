-- client_devops.leader_election definition

CREATE TABLE `leader_election` (
  `anchor` tinyint unsigned NOT NULL,
  `master_id` varchar(128) NOT NULL,
  `last_seen_active` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ip` varchar(128) DEFAULT NULL COMMENT 'ip',
  PRIMARY KEY (`anchor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;