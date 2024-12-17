-- client_devops.task_run definition

CREATE TABLE `task_run` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `task_key` varchar(255)  NOT NULL COMMENT '任务唯一标识',
  `worker_id` varchar(255) NOT NULL COMMENT '工作者id',
  `next_run_at` timestamp NULL DEFAULT NULL COMMENT '下一次执行时间',
  `want_run_status` varchar(255) NOT NULL COMMENT '期望的运行状态: running puased success failed',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `task_key` (`task_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- client_devops.task definition

CREATE TABLE `task` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `task_key` varchar(255)  NOT NULL COMMENT '任务唯一标识',
  `biz_id` varchar(255)  DEFAULT NULL,
  `biz_type` varchar(255) DEFAULT NULL,
  `type` varchar(255)  NOT NULL COMMENT '任务类型',
  `payload` text  NOT NULL COMMENT '任务内容',
  `labels` json DEFAULT NULL COMMENT '任务标签',
  `staints` json DEFAULT NULL COMMENT '任务污点',
  `extra` text,
  `status` varchar(255)  NOT NULL COMMENT 'pending scheduled running|puase success failed',
  `msg` text COMMENT '执行信息',
  `result` json DEFAULT NULL COMMENT '执行结果',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uni_biztype_bizid` (`biz_type`,`biz_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;