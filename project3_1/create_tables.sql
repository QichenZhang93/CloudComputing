USE song_db;
DROP TABLE IF EXISTS `songs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `songs` (
  `track_id` varchar(19) NOT NULL,
  `title` text,
  `song_id` text,
  `release` text,
  `artist_id` text,
  `artist_mbid` text,
  `artist_name` text,
  `duration` double DEFAULT NULL,
  `artist_familiarity` double DEFAULT NULL,
  `artist_hotttnesss` double DEFAULT NULL,
  `year` int(4) DEFAULT NULL,
  PRIMARY KEY (`track_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sales` (
  `track_id` varchar(19) NOT NULL,
  `sales_date` date NOT NULL,
  `sales_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`track_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
