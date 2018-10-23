create table module_running_status(
    `module_name` varchar(255) NOT NULL DEFAULT '',
    `batch_using_time` DOUBLE DEFAULT NULL, -- 分钟
    `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`module_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `config` (
  `name` varchar(100) NOT NULL DEFAULT '',
  `value` varchar(100) DEFAULT NULL,
  `comment` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

--CREATE TABLE `message` (
--  `topic` varchar(200) NOT NULL DEFAULT '',
--  `keyMD5` varchar(200) NOT NULL DEFAULT '',
--  `keySalt` varchar(200) NOT NULL DEFAULT '',
--  `keyBody` varchar(4000) DEFAULT NULL,
--  `data` varchar(4000) DEFAULT NULL,
--  `offset` bigint(20) DEFAULT NULL,
--  `updateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
--  PRIMARY KEY (`topic`,`keyMD5`,`keySalt`)
--) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `message` (
  `topic` varchar(200) NOT NULL DEFAULT '',
  `keyMD5` varchar(200) NOT NULL DEFAULT '',
  `keySalt` varchar(200) NOT NULL DEFAULT '',
  `keyBody` text,
  `data` mediumtext,
  `offset` bigint(20) DEFAULT NULL,
  `updateTime` timestamp NULL DEFAULT NULL,
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`topic`,`keyMD5`,`keySalt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `message_consumer` (
  `consumer` varchar(200) NOT NULL DEFAULT '',
  `topic` varchar(200) NOT NULL DEFAULT '',
  `offset` bigint(20) DEFAULT NULL,
  `updateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`consumer`,`topic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `module_running_status` (
  `module_name` varchar(255) NOT NULL DEFAULT '',
  `batch_using_time` double DEFAULT NULL,
  `batch_actual_time` double DEFAULT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `app_name` varchar(255) DEFAULT NULL,
  `batch_using_time_trace` varchar(5000) DEFAULT NULL,
  `rebrush` varchar(100) DEFAULT NULL COMMENT 'true/false',
  `batch_buration` double DEFAULT NULL,
  PRIMARY KEY (`module_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `offset` (
  `topic` varchar(100) NOT NULL DEFAULT '',
  `partition` varchar(100) NOT NULL DEFAULT '',
  `offset` bigint(20) DEFAULT NULL,
  `module_name` varchar(255) NOT NULL DEFAULT '',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`topic`,`partition`,`module_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `transaction_commited_table` (
  `transaction_id` varchar(255) DEFAULT NULL, -- Partent transaction id
  `is_all_commited` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE rollbackable_transaction_cookie (
  `module_name` varchar(255) NOT NULL DEFAULT '',
  `transaction_parent_id` varchar(255) DEFAULT NULL COMMENT 'Partent transaction id',
  `is_all_commited` varchar(255) DEFAULT NULL COMMENT 'Y/N',
  PRIMARY KEY (`module_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
