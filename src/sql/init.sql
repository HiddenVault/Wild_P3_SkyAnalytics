-- Création de la table 'files'
CREATE TABLE `files` (
    `id` VARCHAR(255) PRIMARY KEY,
    `integre` BOOLEAN
);

-- Création de la table 'aeronefs'
CREATE TABLE `aeronefs` (
    `ref_aero` VARCHAR(255) PRIMARY KEY,
    `type_model` VARCHAR(255),
    `img` VARCHAR(255),
    `debut_service` DATE,
    `last_maint` DATE,
    `en_maintenance` BOOLEAN,
    `end_maint` DATE
);

-- Création de la table 'composants'
CREATE TABLE `composants` (
    `ref_compo` VARCHAR(255) PRIMARY KEY,
    `ref_aero` VARCHAR(255),
    `descr` TEXT,
    `lifespan` INT,
    `taux_usure_actuel` FLOAT,
    `cout_composant` INT,
    FOREIGN KEY (`ref_aero`) REFERENCES `aeronefs` (`ref_aero`)
);

-- Création de la table 'logs_vols' avec suppression en cascade
CREATE TABLE `logs_vols` (
    `id` VARCHAR(255),
    `ref_vol` VARCHAR(255),
    `ref_aero` VARCHAR(255),
    `jour_vol` DATETIME,
    `time_en_air` FLOAT,
    `etat_voyant` INT,
    `temp_C` INT,
    `pressure_hPa` INT,
    `vibrations_ms2` FLOAT,
    CONSTRAINT `fk_ref_aero_logs_vols` FOREIGN KEY (`ref_aero`) REFERENCES `aeronefs` (`ref_aero`),
    CONSTRAINT `fk_id_logs_vols` FOREIGN KEY (`id`) REFERENCES `files` (`id`) ON DELETE CASCADE
);

-- Création de la table 'degradations' avec suppression en cascade
CREATE TABLE `degradations` (
    `id` VARCHAR(255),
    `ref_deg` VARCHAR(255),
    `ref_aero` VARCHAR(255),
    `compo_concerned` VARCHAR(255),
    `usure_cumulee` FLOAT,
    `measure_day` DATE,
    `need_replacement` BOOLEAN,
    CONSTRAINT `fk_ref_aero_degradations` FOREIGN KEY (`ref_aero`) REFERENCES `aeronefs` (`ref_aero`),
    CONSTRAINT `fk_id_degradations` FOREIGN KEY (`id`) REFERENCES `files` (`id`) ON DELETE CASCADE
);
