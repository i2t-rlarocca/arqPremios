DELIMITER $$

USE `suitecrm_cas`$$

DROP FUNCTION IF EXISTS `actualiza_sgte_secuencia_prepag_op`$$

CREATE DEFINER=`sp_funciones`@`localhost` FUNCTION `actualiza_sgte_secuencia_prepag_op`(prov INT(2), sec INT(8)) RETURNS TINYINT(1)
    DETERMINISTIC
    COMMENT 'Actualiza ultima secuencia procesada de pagos otras pcias para la prov del parametro'
BEGIN
	UPDATE sor_rec_preop_par
	SET ult_secuencia = sec
	WHERE provincia = prov
	;
	RETURN TRUE;
    END$$

DELIMITER ;