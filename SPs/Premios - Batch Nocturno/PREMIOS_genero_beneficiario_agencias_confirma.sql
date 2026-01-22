DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_genero_beneficiario_agencias_confirma`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_genero_beneficiario_agencias_confirma`()
BEGIN
	DECLARE done BOOLEAN DEFAULT FALSE;
	DECLARE pid CHAR(36);
	-- tmp_pagos_a_informar es una TABLA TEMPORARIA que se crea en PREMIOS_genero_beneficiario_agencias
	DECLARE c1 CURSOR FOR SELECT idPremio FROM tmp_pagos_a_informar ORDER BY idPremio ASC;
	DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = TRUE;
	OPEN c1;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias_confirma - confirma PREMIO - inicio '));
	c1_loop: LOOP
	FETCH c1 INTO pid;
		-- Si se terminó el cursor, salgo!
		IF `done` THEN 
			LEAVE c1_loop; 
		END IF; 
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias_confirma - confirma PREMIO ', pid));
		-- Mando una 'H' para decir que el canal es AGENCIA, pero no quiero RETORNO (MODO MUDO!!!)
		CALL PREMIOS_confirma_registro('H', pid);
	END LOOP c1_loop;
	
	CLOSE c1;
	
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias_confirma - confirma PREMIO - final '));
    END$$

DELIMITER ;