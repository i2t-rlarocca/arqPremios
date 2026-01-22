DELIMITER $$

USE `suitecrm_cas`$$

DROP FUNCTION IF EXISTS `obtiene_sgte_secuencia_prepag_op`$$

CREATE DEFINER=`sp_funciones`@`localhost` FUNCTION `obtiene_sgte_secuencia_prepag_op`(prov INT(2)) RETURNS INT(8)
    DETERMINISTIC
    COMMENT 'Secuencia para recepcion premios otras pcias'
BEGIN
	/*
	DECLARE sgte_secuencia INT(8);
	SELECT CASE WHEN ISNULL(ult_secuencia) 
		THEN COALESCE(ult_secuencia,1)
		WHEN !ISNULL(ult_secuencia) THEN (ult_secuencia)+1 END AS cant INTO sgte_secuencia
	FROM sor_rec_preop_par  WHERE provincia = prov;
	*/

	-- SET sgte_secuencia = @sgte_secuencia;
	RETURN (SELECT (COALESCE(ult_secuencia,0) + 1) AS sgte_secuencia FROM sor_rec_preop_par  WHERE provincia = prov);
    END$$

DELIMITER ;