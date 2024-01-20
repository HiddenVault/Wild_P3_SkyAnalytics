CREATE TABLE `files` (
  `id` varchar(255) NOT NULL,
  `integre` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `aeronefs` (
  `ref_aero` varchar(255) NOT NULL,
  `type_model` varchar(255) DEFAULT NULL,
  `img` varchar(255) DEFAULT NULL,
  `debut_service` date DEFAULT NULL,
  `last_maint` date DEFAULT NULL,
  `en_maintenance` tinyint(1) DEFAULT NULL,
  `end_maint` date DEFAULT NULL,
  PRIMARY KEY (`ref_aero`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `composants` (
  `ref_compo` varchar(255) NOT NULL,
  `categorie` varchar(255) DEFAULT NULL,
  `ref_aero` varchar(255) DEFAULT NULL,
  `descr` text DEFAULT NULL,
  `lifespan` int(11) DEFAULT NULL,
  `taux_usure_actuel` float DEFAULT NULL,
  `cout` int(11) DEFAULT NULL,
  PRIMARY KEY (`ref_compo`),
  KEY `ref_aero` (`ref_aero`),
  CONSTRAINT `composants_ibfk_1` FOREIGN KEY (`ref_aero`) REFERENCES `aeronefs` (`ref_aero`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `degradations` (
  `id` varchar(255) DEFAULT NULL,
  `ref_deg` varchar(255) DEFAULT NULL,
  `ref_aero` varchar(255) DEFAULT NULL,
  `compo_concerned` varchar(255) DEFAULT NULL,
  `usure_nouvelle` float DEFAULT NULL,
  `measure_day` date DEFAULT NULL,
  `need_replacement` varchar(255) DEFAULT NULL,
  KEY `fk_ref_aero_degradations` (`ref_aero`),
  KEY `fk_id_degradations` (`id`),
  CONSTRAINT `fk_id_degradations` FOREIGN KEY (`id`) REFERENCES `files` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_ref_aero_degradations` FOREIGN KEY (`ref_aero`) REFERENCES `aeronefs` (`ref_aero`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

CREATE TABLE `logs_vols` (
  `id` varchar(255) DEFAULT NULL,
  `ref_vol` varchar(255) DEFAULT NULL,
  `ref_aero` varchar(255) DEFAULT NULL,
  `jour_vol` datetime DEFAULT NULL,
  `time_en_air` float DEFAULT NULL,
  `temp_C` float DEFAULT NULL,
  `pressure_hPa` float DEFAULT NULL,
  `vibrations_ms2` float DEFAULT NULL,
  `etat_voyant` int(11) DEFAULT NULL,
  `sensor_data` text DEFAULT NULL,
  KEY `fk_ref_aero_logs_vols` (`ref_aero`),
  KEY `fk_id_logs_vols` (`id`),
  CONSTRAINT `fk_id_logs_vols` FOREIGN KEY (`id`) REFERENCES `files` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_ref_aero_logs_vols` FOREIGN KEY (`ref_aero`) REFERENCES `aeronefs` (`ref_aero`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;