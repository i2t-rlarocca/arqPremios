DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_conexion_tesoreria_Pagos`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_conexion_tesoreria_Pagos`(
		  IN opp_name CHAR(36), 
		  OUT RCode INT, 
		  OUT RTxt LONGTEXT,
		  OUT RId VARCHAR (36),
		  OUT RSQLErrNo INT,
		  OUT RSQLErrtxt VARCHAR (500)
		  
)
thisSP:BEGIN
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		ROLLBACK;
		
		  GET DIAGNOSTICS CONDITION 1
		    @code = RETURNED_SQLSTATE,
		    @msg = MESSAGE_TEXT,
		    @errno = MYSQL_ERRNO,
		    @base = SCHEMA_NAME,
		    @tabla = TABLE_NAME;
		  SET RCode = 0;
		  SET RTxt = "";
		  SET RId = 0;
		  SET RSQLErrNo = @errno;
		  SET RSQLErrtxt = CONCAT("PREMIOS_conexion_tesoreria_Pagos ", @msg);
		
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_pagos - FIN CON ERROR - etapa ', @etapa, ' - Orden de Pago:', opp_name, 
				' - code:', @code, ' - errno:', @errno, ' - msg:', @msg));
				
		SET 	RCode = @code,
			RTxt = "",
			RId = "",
			RSQLErrNo = @errno,
			RSQLErrtxt = CONCAT(@msg, ' en tabla ', @tabla, ' de ', @base, '  - llegó a etapa: ', @etapa) ;
	END;
	
	SET RCode = 1 ;
	SET RTxt = 'OK' ;
	SET RSQLErrNo = 0 ;
	SET RSQLErrtxt = "OK" ;
	SET RId = 0 ;
	
	
	SET @etapa = 0;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_pagos - INI - ', @etapa, ' - Orden de Pago:', opp_name));
	
	SET @etapa = 1;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_pagos - Actualiza Premios y OP - ', @etapa, ' - Orden de Pago:', opp_name));
	
	-- recupera datos de la liquidacion
	SELECT a.fecha, a.created_by,  d.numerovalor, d.tg01_cajas_id1_c AS idcuentabancaria_cas, f.nombrecorto AS tipomediopago,
		 COALESCE(h.id,'') AS idbanco, COALESCE(e.cbu_c,'') AS cbu, COALESCE(e.cuentabancaria_c,'') AS cuentabancaria, 
		 COALESCE(e.tipoctabancaria_c,'0' ) AS tipoctabancaria, a.description 
	INTO @fecha, @created_by,  @numerovalor, @idcuentabancaria_cas, @tipomediopago, @idbanco, @cbu, 
		@cuentabancaria, @tipoctabancaria, @description
	FROM suitecrm_administracion_2019.tg12_liquidacion a
		 INNER JOIN suitecrm_administracion_2019.`tg12_liquidacion_mediospagos_tg12_liquidacion_c` b ON a.id = b.`tg12_liqui4421idacion_ida`
		 AND b.deleted = 0
		 INNER JOIN suitecrm_administracion_2019.`tg12_liquidacion_mediospagos` c ON c.id = b.`tg12_liqui1c84ospagos_idb`
		 INNER JOIN suitecrm_administracion_2019.tg05_carteradevalores d ON d.id = tg05_carteradevalores_id_c
		 INNER JOIN suitecrm_administracion_2019.accounts_cstm e ON e.id_c = d.account_id2_c
		 INNER JOIN suitecrm_administracion_2019.tg01_tipomediopago f ON f.id = d.tg01_tipomediopago_id_c
		 LEFT JOIN  suitecrm_administracion_2019.tg01_bancos g ON g.id = d.tg01_bancos_id_c
		 LEFT JOIN  tb2_bancos h ON h.bco_identificador = g.idbanco
	 WHERE a.name = opp_name LIMIT 1;
	 	 
	START TRANSACTION;
	
	-- actualiza el premio
		
	UPDATE pre_premios  pr
	INNER JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pr.id AND opp.deleted = 0
	-- INNER JOIN suitecrm_administracion_2019.tg12_liquidacion vpag ON vpag.name = opp.name
	SET pr.pre_estadopago = 'A' 
		, opp.modified_user_id =  @created_by
		, opp.date_modified = CONCAT(CURDATE(), ' 11:00:00') -- now()
		, pre_estcontabilizacion = 'C'
	WHERE pr.deleted = 0
	 AND (opp.name = opp_name)
	 AND opp.estado_envio_as400 = 'E'
	 AND opp.pre_canal_de_pago_id_c = 'T'
	 AND opp.opp_estado_actualizacion_fpag <> 'C'
	;
	
	 
	 SET @opp_fpago = 1; -- Efectivo
	 SET @opp_tipocuenta = '';
	 SET @opp_ncuenta = '';
	 SET @opp_cbu = '';
	 SET @opp_numero_cheque = 0;
	 SET @opp_banco = '';
	 
	 IF @tipomediopago = 'TFR' THEN
		 SET @opp_fpago = 2;
		 SET @opp_tipocuenta = @tipoctabancaria;
		 SET @opp_ncuenta = @cuentabancaria;
		 SET @opp_cbu = @cbu;
		 SET @opp_numero_cheque = @numerovalor;
		 
		 -- lmariotti 9-4-2024, recupera el banco desde el CBU

		 SELECT COALESCE(id,'') INTO @idbanco FROM tb2_bancos  
		 WHERE SUBSTR(@cbu, 1, 3) = LPAD(bco_identificador,3,'000'); 
		 
		 SET @opp_banco = @idbanco; -- Agregado AE - 2023-10-03
		 
	 END IF;
	 
	 IF (@tipomediopago = 'CHP' OR @tipomediopago = 'CHT') THEN
	 	 SET @opp_fpago = 3;
		 SET @opp_numero_cheque = @numerovalor;
		 SET @opp_banco = @idbanco;
	 END IF;
	 
	 -- actualiza orden de pago
	 
	 UPDATE pre_orden_pago opp
	 SET
				  opp.opp_fpago =  @opp_fpago
				-- , opp.opp_fecha_pago = @fecha
				, opp.opp_fecha_pago = CONCAT(@fecha, ' 11:00:00')
				, opp.opp_tipocuenta = @opp_tipocuenta
				, opp.opp_ncuenta = @opp_ncuenta
				, opp.opp_cbu = @opp_cbu
				, opp.opp_numero_cheque = @opp_numero_cheque
				, tb2_bancos_id_c = @opp_banco
				, opp.modified_user_id =  @created_by
				, opp.date_modified = CONCAT(CURDATE(), ' 11:00:00') -- now()
				, opp.description = @description
				, opp.opp_estado_actualizacion_fpag = 'C'
	WHERE opp.name = opp_name
		 AND opp.estado_envio_as400 = 'E'
		 AND opp.pre_canal_de_pago_id_c = 'T'
		 AND opp.opp_estado_actualizacion_fpag <> 'C'
	;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_pagos - actualiza Orden de Pago:', opp_name));
	-- actualiza estado del documento, para registrar en premios el recibo
	
	UPDATE pre_orden_pago a
	INNER JOIN pre_orden_pago_documentos b ON  a.id = b.pre_orden_pago_id_c AND b.deleted = 0
	SET b.opd_estado = 'C'
	WHERE a.name = opp_name;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_pagos - actualiza estado del documento Orden de Pago:', opp_name));		
	
	COMMIT;	
	
	SET @etapa = 3;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria_pagos - FIN - ', @etapa, ' - Orden de Pago:', opp_name));
	
	SET  RId = opp_name, 
	     RCode = 1, 
	     RTxt = 'OK' ;
    END$$

DELIMITER ;