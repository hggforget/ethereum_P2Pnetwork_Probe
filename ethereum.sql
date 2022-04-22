/*
 Navicat Premium Data Transfer

 Source Server         : 36
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.1.36:3306
 Source Schema         : topo_p2p

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 16/03/2021 11:09:19
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for ethereum
-- ----------------------------
DROP TABLE IF EXISTS `ethereum`;
CREATE TABLE `ethereum`  (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `nodeid` char(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `publickey` char(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `pingtime` int(10) UNSIGNED NULL DEFAULT 0,
  `port` int(11) NULL DEFAULT 0,
  `ip` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `pongtime` int(10) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `noideid`(`nodeid`) USING BTREE,
  INDEX `publickey`(`publickey`) USING BTREE,
  INDEX `pingtime`(`pingtime`) USING BTREE,
  INDEX `pongtime`(`pongtime`) USING BTREE,
  UNIQUE INDEX `r`(`nodeid`,`port`,`ip`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1281456 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for ethereum_neighbours
-- ----------------------------
DROP TABLE IF EXISTS `ethereum_neighbours`;
CREATE TABLE `ethereum_neighbours`  (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `nodeid1` char(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `nodeid2` char(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP,
  `intTIME` int(10) UNSIGNED NULL DEFAULT 0,
  `RECV_NUM` int(10) UNSIGNED NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `date_time`(`nodeid1`, `update_time`) USING BTREE,
  UNIQUE INDEX `r`(`nodeid1`, `nodeid2`,`RECV_NUM`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 998318 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;


DROP TABLE IF EXISTS `ethereum_active_nodes`;
CREATE TABLE `ethereum_active_nodes`  (
  `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `nodeid` char(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `publickey` char(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `pingtime` int(10) UNSIGNED NULL DEFAULT 0,
  `port` int(11) NULL DEFAULT 0,
  `ip` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `pongtime` int(10)  NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `noideid`(`nodeid`) USING BTREE,
  INDEX `publickey`(`publickey`) USING BTREE,
  INDEX `pingtime`(`pingtime`) USING BTREE,
  INDEX `pongtime`(`pongtime`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1281456 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
