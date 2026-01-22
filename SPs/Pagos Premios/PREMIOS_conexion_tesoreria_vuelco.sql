DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_conexion_tesoreria_vuelco`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_conexion_tesoreria_vuelco`(
		  OUT RCode INT, 
		  OUT RTxt LONGTEXT,
		  OUT RId VARCHAR (36),
		  OUT RSQLErrNo INT,
		  OUT RSQLErrtxt VARCHAR (500)  
)
    COMMENT 'Ejecuto el vuelco masivo de premios a tesoreria'
thisSP:BEGIN
	-- cursor de minutas
	DECLARE premios_a_volcar CURSOR FOR
		SELECT p.id AS idPrm, op.name AS idLiquidacion
			FROM pre_premios p 
				JOIN pre_orden_pago op ON op.pre_premios_id_c = p.id AND op.deleted = 0 
			WHERE p.pre_estregistrobenef = 'C' 
				AND p.pre_estadopago= 'A'
				AND COALESCE(op.estado_envio_as400, 'P') = 'P' 
				AND op.opp_fecha_comprobante > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) 
				-- AND op.opp_fecha_pago > '2023-09-30' --  // solo tratamiento de premios pagados desde el 1/10/2023
			ORDER BY op.name;
	-- Manejador de errores
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1
			@code = RETURNED_SQLSTATE,
			@msg = MESSAGE_TEXT,
			@errno = MYSQL_ERRNO,
			@base = SCHEMA_NAME,
			@tabla = TABLE_NAME;
			
		SET RCode = 0;
		SET RTxt = "Excepción MySQL";
		SET RId = 0;
		SET RSQLErrNo = @errno;
		SET RSQLErrtxt = CONCAT("PREMIOS_conexion_tesoreria ", @msg);
			
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_vuelco - FIN CON ERROR - etapa ', @etapa, ' - Orden de Pago:', opp_id, 
				' - code:', @code, ' - errno:', @errno, ' - msg:', @msg));
	END;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_vuelco - INICIO'));
	
	SET RCode = 1;
	SET RTxt = 'OK';
	SET RId = 0;
	SET RSQLErrNo = 0;
	SET RSQLErrtxt = "OK";
		
	SET @etapa = 0, @errores = 0;
	OPEN premios_a_volcar;
	
	iterator:LOOP
		BEGIN
			-- Variable para manejar el fin de archivo
			DECLARE exit_loop  BOOLEAN DEFAULT FALSE;
			DECLARE idPrm  	   VARCHAR(36);
			DECLARE idLiquidacion VARCHAR(255);
			-- Fin de archivo
			DECLARE CONTINUE HANDLER FOR NOT FOUND
			BEGIN
				SET exit_loop = TRUE;
			END;
			-- Leo el proximo registro...
			FETCH NEXT FROM premios_a_volcar 
					INTO idPrm, idLiquidacion;
			-- Si se detecto fin de archivo, termino!
			IF exit_loop THEN 
				LEAVE iterator;
			END IF;
			-- aca logueo todos los campos del cursor
			INSERT INTO kk_auditoria(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria_vuelco - Vuelco premio: ', idPrm, ' - liquidacion: ', idLiquidacion));
			CALL PREMIOS_conexion_tesoreria(idPrm, @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
			-- Si hubo error, log y SIGUE!
			IF (@RCode <> 1) THEN
				SET @errores = @errores + 1;
				INSERT INTO kk_auditoria(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria_vuelco - Error en Vuelco premio: ', idPrm, 
											' - liquidacion: ', idLiquidacion, ' - error: ', @RCode,
											' - mensaje: ', @RTxt));
				/* por ahora no corto el proceso!!!
				SET 	RCode      = @RCode,
					RTxt       = @RTxt,
					RId        = '',
					RSQLErrNo  = @RSQLErrNo,
					RSQLErrtxt = @RSQLErrtxt;
				LEAVE iterator;
				*/
			END IF;
		END;	
	END LOOP iterator;
	
	CLOSE premios_a_volcar;
	
	/*
	22-5-2024 - lmariotti
	- se incorpora trazabilidad entre las minutas contables, liquidaciones de premios y retenciones de premios
	- en tg03_comprobantes y tg05_retenciones_y_precepciones se agrego el atributo de identificador de proceso
	- esto permite en contabilidad, comparar minutas contables y retenciones previo al envio al sicore. Tiene que haber cuadratura
	- para evitar impactar en varios SP de administracion para la asignacion del id de proceso, que solo tiene efecto para premios
	*/
	
	INSERT INTO kk_auditoria(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria_vuelco - UPDATE identificador de proceso premios-retenciones '));	
	
	-- asigna identificador de proceso de premios en minutas de premios
	
	UPDATE	cas02_cc_cntpoc_grp r
	INNER JOIN cas02_cc_cntpoc_cab c ON c.cab_identprc = r.grp_identprc AND c.deleted = 0
	INNER JOIN suitecrm_administracion_2019.tg03_comprobantes d ON d.id = c.tg03_comprobantes_id_c
	SET d.id_proceso = LEFT(c.cab_identprc,24)
	WHERE r.deleted = 0 AND r.grp_identprc LIKE 'ppag%'
	AND grp_feccon >=   DATE_SUB(CURDATE(), INTERVAL 3 DAY) ;
	
	-- asigna identificador de proceso de premios en constancias de retencion

	UPDATE pre_resumen_pagos_det a
	INNER JOIN suitecrm_administracion_2019.tg05_retenciones_y_precepciones b ON b.nrofactura = a.opp_name AND b.deleted = 0
	SET b.id_proceso = CONCAT(a.pag_prcpagpres,'_',a.pag_fecha)
	WHERE a.pag_fecha >= DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND a.deleted = 0;
	
	-- Mensaje de fin!
	IF (@errores = 0) THEN
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_vuelco - FIN OK'));
	ELSE
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_vuelco - FIN con errores. No se pudieron volcar ', @errores, ' premios.'));
	END IF;
	
END$$

DELIMITER ;