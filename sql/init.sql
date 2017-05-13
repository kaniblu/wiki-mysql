DROP TABLE IF EXISTS `articles`;
DROP TABLE IF EXISTS `bodies`;


CREATE TABLE `bodies` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `body` mediumtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `articles` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `aid` int(11) NOT NULL,
  `title` varchar(1027) NOT NULL,
  `body` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `body` (`body`),
  CONSTRAINT `articles_ibfk_1` FOREIGN KEY (`body`) REFERENCES `bodies` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;