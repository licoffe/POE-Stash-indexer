#
# SQL Export
# Created by Querious (1055)
# Created: December 12, 2016 at 16:45:26 GMT+1
# Encoding: Unicode (UTF-8)
#


DROP DATABASE IF EXISTS `POE`;
CREATE DATABASE `POE` DEFAULT CHARACTER SET latin1 DEFAULT COLLATE latin1_swedish_ci;
USE `POE`;




SET @PREVIOUS_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS;
SET FOREIGN_KEY_CHECKS = 0;


DROP TABLE IF EXISTS `Stashes`;
DROP TABLE IF EXISTS `Sockets`;
DROP TABLE IF EXISTS `Requirements`;
DROP TABLE IF EXISTS `Properties`;
DROP TABLE IF EXISTS `Mods`;
DROP TABLE IF EXISTS `Items`;
DROP TABLE IF EXISTS `CurrencyStats`;
DROP TABLE IF EXISTS `Currencies`;
DROP TABLE IF EXISTS `Leagues`;
DROP TABLE IF EXISTS `ChangeId`;
DROP TABLE IF EXISTS `Accounts`;


CREATE TABLE `Accounts` (
  `accountName` varchar(128) NOT NULL DEFAULT '',
  `lastCharacterName` varchar(128) DEFAULT NULL,
  `lastSeen` bigint(20) DEFAULT '0',
  PRIMARY KEY (`accountName`),
  UNIQUE KEY `accountName` (`accountName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `ChangeId` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nextChangeId` varchar(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`,`nextChangeId`)
) ENGINE=InnoDB AUTO_INCREMENT=4926 DEFAULT CHARSET=utf8;


CREATE TABLE `Leagues` (
  `leagueName` varchar(128) NOT NULL DEFAULT '',
  `active` tinyint(4) DEFAULT '0',
  `poeTradeId` varchar(128) DEFAULT '',
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`leagueName`),
  UNIQUE KEY `leagueName` (`leagueName`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=229425 DEFAULT CHARSET=utf8;


CREATE TABLE `Currencies` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` bigint(20) NOT NULL DEFAULT '0',
  `league` varchar(128) NOT NULL DEFAULT '',
  `sell` varchar(128) NOT NULL DEFAULT '',
  `currencyKey` varchar(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`currencyKey`,`id`),
  UNIQUE KEY `currencyKey` (`currencyKey`),
  UNIQUE KEY `id` (`id`),
  KEY `sell` (`sell`) USING BTREE,
  KEY `league` (`league`) USING BTREE,
  CONSTRAINT `Currencies_ibfk_1` FOREIGN KEY (`league`) REFERENCES `Leagues` (`leagueName`)
) ENGINE=InnoDB AUTO_INCREMENT=1095 DEFAULT CHARSET=utf8;


CREATE TABLE `CurrencyStats` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `buy` varchar(128) NOT NULL DEFAULT '',
  `mean` float DEFAULT '0',
  `median` float DEFAULT '0',
  `mode` float DEFAULT '0',
  `min` float DEFAULT '0',
  `max` float DEFAULT '0',
  `currencyKey` varchar(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `buy` (`buy`) USING BTREE,
  KEY `currencyKey` (`currencyKey`) USING BTREE,
  CONSTRAINT `currencystats_ibfk_1` FOREIGN KEY (`currencyKey`) REFERENCES `Currencies` (`currencyKey`)
) ENGINE=InnoDB AUTO_INCREMENT=18206 DEFAULT CHARSET=utf8;


CREATE TABLE `Items` (
  `w` tinyint(4) NOT NULL DEFAULT '0',
  `h` tinyint(4) NOT NULL DEFAULT '0',
  `ilvl` smallint(6) NOT NULL DEFAULT '0',
  `icon` varchar(128) NOT NULL DEFAULT '',
  `league` varchar(128) NOT NULL DEFAULT '',
  `itemId` varchar(128) NOT NULL DEFAULT '',
  `name` varchar(128) DEFAULT NULL,
  `typeLine` varchar(128) DEFAULT NULL,
  `identified` tinyint(4) NOT NULL DEFAULT '0',
  `verified` tinyint(4) NOT NULL DEFAULT '0',
  `corrupted` tinyint(4) NOT NULL DEFAULT '0',
  `lockedToCharacter` tinyint(4) DEFAULT '0',
  `frameType` tinyint(4) DEFAULT '0',
  `x` tinyint(4) DEFAULT '0',
  `y` tinyint(4) DEFAULT '0',
  `inventoryId` varchar(128) DEFAULT NULL,
  `accountName` varchar(128) NOT NULL DEFAULT '',
  `stashId` varchar(128) NOT NULL DEFAULT '',
  `socketAmount` tinyint(4) NOT NULL DEFAULT '0',
  `linkAmount` tinyint(4) NOT NULL DEFAULT '0',
  `available` tinyint(4) NOT NULL DEFAULT '0',
  `addedTs` bigint(20) DEFAULT '0',
  `updatedTs` bigint(20) DEFAULT '0',
  `flavourText` varchar(1024) DEFAULT NULL,
  `price` varchar(128) DEFAULT NULL,
  `crafted` tinyint(4) DEFAULT '0',
  `enchanted` tinyint(4) DEFAULT '0',
  PRIMARY KEY (`itemId`),
  UNIQUE KEY `itemId` (`itemId`),
  KEY `league` (`league`),
  KEY `accountName` (`accountName`),
  KEY `stashId` (`stashId`),
  KEY `idx_name` (`name`) USING BTREE,
  KEY `idx_typeLine` (`typeLine`) USING BTREE,
  CONSTRAINT `Items_ibfk_1` FOREIGN KEY (`league`) REFERENCES `Leagues` (`leagueName`),
  CONSTRAINT `Items_ibfk_2` FOREIGN KEY (`accountName`) REFERENCES `Accounts` (`accountName`),
  CONSTRAINT `Items_ibfk_3` FOREIGN KEY (`stashId`) REFERENCES `Stashes` (`stashID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `Mods` (
  `itemId` varchar(128) DEFAULT NULL,
  `modName` varchar(128) NOT NULL DEFAULT '0',
  `modValue1` smallint(6) DEFAULT '0',
  `modValue2` smallint(6) DEFAULT '0',
  `modValue3` smallint(6) DEFAULT '0',
  `modValue4` smallint(6) DEFAULT '0',
  `modType` enum('EXPLICIT','IMPLICIT','CRAFTED','ENCHANTED') DEFAULT 'IMPLICIT',
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `modKey` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `modKey` (`modKey`),
  KEY `itemId` (`itemId`) USING BTREE,
  KEY `modName` (`modName`) USING BTREE,
  KEY `modType` (`modType`) USING BTREE,
  KEY `idx_itemId_modName` (`itemId`,`modName`) USING BTREE,
  CONSTRAINT `Mods_ibfk_1` FOREIGN KEY (`itemId`) REFERENCES `Items` (`itemId`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=20174836 DEFAULT CHARSET=utf8;


CREATE TABLE `Properties` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `itemId` varchar(128) DEFAULT NULL,
  `propertyName` varchar(128) NOT NULL DEFAULT '0',
  `propertyValue1` smallint(6) DEFAULT '0',
  `propertyValue2` smallint(6) DEFAULT '0',
  `propertyKey` varchar(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `propertyKey` (`propertyKey`),
  KEY `itemId` (`itemId`),
  KEY `Properties_ibfk_2` (`propertyName`),
  CONSTRAINT `Properties_ibfk_1` FOREIGN KEY (`itemId`) REFERENCES `Items` (`itemId`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=15462426 DEFAULT CHARSET=utf8;


CREATE TABLE `Requirements` (
  `itemId` varchar(128) DEFAULT NULL,
  `requirementName` varchar(128) NOT NULL DEFAULT '0',
  `requirementValue` smallint(6) DEFAULT '0',
  `requirementKey` varchar(128) NOT NULL DEFAULT '',
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  UNIQUE KEY `requirementKey` (`requirementKey`),
  UNIQUE KEY `id` (`id`),
  KEY `itemId` (`itemId`) USING BTREE,
  KEY `requirementName` (`requirementName`) USING BTREE,
  CONSTRAINT `Requirements_ibfk_1` FOREIGN KEY (`itemId`) REFERENCES `Items` (`itemId`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=10033809 DEFAULT CHARSET=utf8;


CREATE TABLE `Sockets` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `itemId` varchar(128) DEFAULT NULL,
  `socketGroup` tinyint(4) DEFAULT '0',
  `socketAttr` char(1) DEFAULT NULL,
  `socketKey` varchar(128) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `socketKey` (`socketKey`),
  UNIQUE KEY `unnamedColumn` (`id`),
  KEY `itemId` (`itemId`) USING BTREE,
  CONSTRAINT `Sockets_ibfk_1` FOREIGN KEY (`itemId`) REFERENCES `Items` (`itemId`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5617225 DEFAULT CHARSET=utf8;


CREATE TABLE `Stashes` (
  `stashId` varchar(128) NOT NULL DEFAULT '',
  `stashName` varchar(128) DEFAULT NULL,
  `stashType` varchar(128) DEFAULT NULL,
  `publicStash` tinyint(4) DEFAULT '0',
  PRIMARY KEY (`stashId`),
  UNIQUE KEY `stashID` (`stashId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




SET FOREIGN_KEY_CHECKS = @PREVIOUS_FOREIGN_KEY_CHECKS;


