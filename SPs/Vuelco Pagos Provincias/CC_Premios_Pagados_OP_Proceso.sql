DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `CC_Premios_Pagados_OP_Proceso`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `CC_Premios_Pagados_OP_Proceso`(IN id_proceso INT,IN opc VARCHAR(3),OUT msgret VARCHAR(2048),OUT msgaux VARCHAR(2048))
fin:BEGIN
	-- declaro variables para manejadores y cursores...
	DECLARE done INT DEFAULT 0;
	DECLARE pid CHAR(36);
	-- variables cursor pagos. OJO! las variables para los fetchs no pueden ser con @...
	DECLARE juego VARCHAR(2);
	DECLARE sorteo VARCHAR(8);
	DECLARE ticket VARCHAR(10);
	DECLARE provincia VARCHAR(2);
	DECLARE permiso INT(6);
	DECLARE account_id VARCHAR(36);
	DECLARE agencia VARCHAR(5);
	DECLARE subagencia VARCHAR(3);
	DECLARE fecha_pago VARCHAR(8);
	DECLARE hora_pago VARCHAR(6);
	DECLARE en_premios VARCHAR(1);
	DECLARE en_premios_menores VARCHAR(1);
	DECLARE id_premio VARCHAR(36);
	DECLARE idpgmsorteo VARCHAR(36);
	DECLARE pre_estenvioretencion VARCHAR(100);
	DECLARE pre_estregistrobenef VARCHAR(100);
	-- variables cursor resumen emision. OJO! las variables para los fetchs no pueden ser con @...
	DECLARE idjuego2 VARCHAR (2);
	DECLARE sorteo2 VARCHAR(8);
	DECLARE idpgmsorteo2 VARCHAR(36);	
	DECLARE cod_error VARCHAR(50);
	DECLARE mensaje VARCHAR(255) DEFAULT '';
	DECLARE v_prv INT(2);
	DECLARE v_sec INT(8);	

	-- declaro cursores...
	DECLARE mi_cursor_RR CURSOR FOR SELECT rr.id_premio FROM sor_rec_preop rr WHERE cod_err = '0' ORDER BY rr.id_premio ASC;

	-- declaro manejadores...
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN	
		GET DIAGNOSTICS CONDITION 1
			@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
			@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
	
		SET cod_error = CASE WHEN COALESCE(@errno,0) !=0 THEN @errno ELSE 1 END;
		SET mensaje = CONCAT('Ocurrió un problema durante el procesamiento de los pagos. Consulte a Soporte.');
		
		ROLLBACK;
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Proceso - id_proceso: ', id_proceso, ' - opc: ', opc);
		
		-- Retorno
		SELECT cod_error, mensaje;
	END;
	
	-- inicializo variables de trabajo..
	SET msgret = 'rr';
	SET msgaux = 'mm';
	
	SET cod_error = '0';
	SET mensaje = 'OK';
	SET @juego  = '';
	SET @sorteo  = '';
	SET @ticket  = '';
	SET @provincia  = '';
	SET @permiso = 0;
	SET @account_id  = '';
	SET @agencia  = '';
	SET @subagencia  = '';
	SET @fecha_pago  = '';
	SET @hora_pago  = '';	
	SET @en_premios  = 'N';	
	SET @en_premios_menores  = 'N';	
	
	SET @id_premio = '';
	SET @estado_retencion = '';
	SET @estado_beneficiario = '';
	SET @id_pgmsorteo = '';
	SET @id_provi ='';
	SET @fecha_hora = NOW();
	SET @estado_emision_ddjj = '';
	SET @cadena ='';
	SET @resultado = 0; -- Para contar los resultados del select
	SET @idjuego2 = '';
	SET @sorteo2 = '';
	SET @idpgmsorteo2 = '';    
	SET @pid = '';

	SET @hoy = NOW();
	
	--   actualizar PREMIOS 
	--                    	pre_estadopago <- 'A' 
	--			date_modified 
	
	IF (opc = 'IN')	THEN
	-- 1
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Proceso - paso 1 - insert Oreden Pago');
		INSERT INTO pre_orden_pago
				(`id`,
				     `name`,
				     `date_entered`, `date_modified`,
				     `modified_user_id`, `created_by`, `description`, `deleted`, `assigned_user_id`,
				     `pre_tipo_comprobante_id_c`,
				     `pre_talonarios_id_c`,
				     `opp_letra`,
				     `opp_punto_venta`,
				     `opp_numero`,
				     `opp_fecha_pago`,
				     `opp_fecha_comprobante`,
				     `pre_premios_id_c`,
				     `pre_premios_cuotas_id_c`,
				     `tbl_provincias_id_c`,
				     `account_id_c`,
				     `opp_agente_pago`, `opp_subagente_pago`,
				     `opp_impbruto`, `opp_impimponible`, `opp_impneto`, `opp_ret_ley20630`, `opp_ret_ley23351`, `opp_ret_ley11265`, `opp_ret_otros`,
				     `opp_fpago`, `opp_tipocuenta`, `opp_ncuenta`, `opp_cbu`, `opp_numero_cheque`, `opp_numero_retencion`, 
				     `opp_estado_registracion`,
				     `opp_estado_emision_ddjj`,
				     `opp_estado_actualizacion_fpag`,
				     `opp_impotros`,
				     `opp_fecha_emision_ddjj`,
				     `tb2_bancos_id_c`)	
		SELECT
				p.id, 
				CONCAT('provisorio-', LEFT(p.id,26)) AS NAME, -- pendiente de asignar numero
				@hoy, @hoy, 1, 1, 
				NULL AS description,
				0, 1, 
				NULL,  -- tipo de comprobante (en modo provisorio no existe) ???
				NULL,  -- talonario (como lo calculo) ???
				'',    -- letra ???
				0, 0,  -- punto de venta  y numero (como?)
				CONCAT(pop.fecha_pago), -- Corregido RL 20/02/2018. Error en la corrección de AE 19/2/2018
				NULL,  -- la fecha de comprobante es la fecha actual ??? -- RR
				-- '2021-11-24',  -- la fecha de comprobante es la fecha actual ??? -- RR
				p.id,  -- la conexion al premio
				'',    -- NO SON PREMIOS CUOTA
				prv.id AS tbl_provincia_id_c, 	 -- PROVINCIA DE PAGO
				cs.id_c,
				0,-- pp.agente,                       -- AGENCIA DE PAGO
				0,-- pp.subagente,                    -- subagente de pago
				p.pre_impbruto, p.pre_impimponible, p.pre_impneto, p.pre_ret_ley20630, p.pre_ret_ley23351, p.pre_ret_ley11265, p.pre_ret_otros,
				pop.forma_pago, -- forma de pago
				pop.tipo_cuenta, -- tipo de cuenta
				pop.numero_cuenta, -- numero de cuenta
				pop.cbu, -- cbu
				pop.numero_cheque, -- numero de cheque
				0, -- numero de retencion
				'P', -- estado de registracion (Provisorio)
				'P', -- estado de emision de DDJJ
				'C', -- estado de actualizacion de FECHA DE PAGO
				p.pre_impotros,
				NULL, -- fecha de emision de DDJJ
				IF(pop.forma_pago IN (2,3), pop.codigo_banco,0) -- codigo de banco	
		FROM sor_rec_preop pop
			INNER JOIN sor_pgmsorteo ps ON ps.idjuego = pop.numero_juego AND ps.nrosorteo = pop.numero_sorteo AND ps.deleted = 0
			INNER JOIN tbl_provincias prv ON prv.prv_id_boldt = pop.provincia AND prv.deleted = 0
			INNER JOIN pre_premios p ON p.sor_pgmsorteo_id_c = ps.id 
						AND p.tbl_provincias_id_c = prv.id
						AND p.pre_secuencia = pop.ocr
						AND p.pre_clasepremio = 'B' AND p.pre_estadopago = 'E' -- estado pago pendiente
						AND p.deleted = 0
			INNER JOIN accounts_cstm cs ON cs.id_permiso_c = pop.provincia  AND cs.categoria_c='Provincia'
		WHERE pop.cod_err = '0'
		;		
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Proceso - paso 2 - FINALIZO INSERT Oreden Pago');
		
		-- inser beneficiarios
		INSERT INTO `pre_orden_pago_beneficiarios`
			    (`id`,
			     `name`,
			     `date_entered`, `date_modified`, `modified_user_id`, `created_by`, `description`, `deleted`, `assigned_user_id`,
			     `pre_orden_pago_id_c`,
			     `opb_tipobeneficiario`, 
			     `opb_tipo`, `opb_cuit`,
			     `opb_nombre`, `opb_snombre`, `opb_apellido`, `opb_apemat`,
			     `opb_tdoc`, `opb_doc_nro`,
			     `opb_domi`, `opb_domi_calle`, `opb_domi_nro`, `opb_domi_piso`, `opb_domi_dpto`,
			     `opb_sexo`, `opb_fechanac`, `opb_estado_civil`, 
			     `opb_email`, `opb_telefono_carac`, `opb_telefono_numero`,
			     `opb_pep`, `opb_cargo`,
			     `tbl_provincias_id_c`, `tbl_localidades_id_c`, `tbl_nacionalidades_id_c`, `tbl_ocupaciones_id_c`, `tbl_paises_id_c`,
			     `tipo_documental_afip`, `opb_tipo_soc`, `opb_obs`,
			     `opb_extranjero`, `opb_provincia_extranjera`, `opb_localidad_extranjera`, `opb_cpos_extranjero`, 
			     `pre_envios_uif_id_c`, `opb_gan_tipo_identif`
			     )
		SELECT CONCAT(LEFT(p.id,35), 'B'), -- id (beneficiario)
			COALESCE(rpp.benef_cuit_cuil,rpp.num_doc_persona_human) COLLATE utf8_spanish_ci , -- Nombre
			@hoy, @hoy, 1, 1, NULL, 0, 1,
			p.id, -- orden de pago, igual que el premio
			'B',  -- tipo de beneficiario ("B")
			COALESCE(rpp.tipo_benef,''), -- tipo de persona
			rpp.benef_cuit_cuil, 
			COALESCE(SUBSTRING_INDEX(rpp.benef_nombre, ' ', 1),''), -- primer nombre
			COALESCE(SUBSTRING_INDEX(rpp.benef_nombre, ' ', -1), ''), -- segundo nombre
			COALESCE(rpp.benef_apellido, ''),
			COALESCE(rpp.benef_segundo_nombre, ''), 
			COALESCE(rpp.benef_tipo_documento,rpp.benef_tipo_cuit_afip),
			CAST(COALESCE(rpp.num_doc_persona_human, SUBSTR(rpp.benef_cuit_cuil,3,8)) AS UNSIGNED), 
			COALESCE(rpp.benef_calle_nombre, ''),
			COALESCE(rpp.benef_calle_nombre, ''), 
			COALESCE(rpp.benef_calle_numero, ''), 
			COALESCE(rpp.benef_calle_piso, ''), 
			COALESCE(rpp.benef_calle_depto, ''),
			COALESCE(rpp.benef_sexo, ''), 
			DATE(COALESCE(DATE_FORMAT(rpp.benef_fecha_nac_ini_actividades,'%Y-%m-%d'), NULL)), 
			COALESCE(rpp.benef_estado_civil, '0'), -- SI NO VIENE, ES 0 = NO INFORMADO
			COALESCE(rpp.benef_email,''), 
			COALESCE(rpp.benef_telef_carac, ''), 
			COALESCE(rpp.benef_telef, ''),
			COALESCE(rpp.benef_pep,'N'), 
			COALESCE(rpp.benef_cargo,''),
			COALESCE(rpp.id_provincia,''), -- provincia
			COALESCE(rpp.id_localidad,''), -- localidad
			COALESCE(rpp.id_nacionalidad, '1'), -- nacionalidad SI NO VIENE, ES 1 ARGENTINA
			COALESCE(rpp.id_ocupacion, 'PERMISIONARIO'), -- ocupacion SI NO VIENE, ES PERMISIONARIO
			COALESCE(rpp.id_pais,'1'), -- pais de residencia
			COALESCE(rpp.benef_tipo_cuit_afip, '86'), -- tipo documental afip (por defecto CUIL 86)
			COALESCE(rpp.benef_tipo_sociedad, ''), -- tipo de sociedad
			'', -- observaciones
			0,  
			'', 
			'', 
			'', 
			'',
			''
	
		FROM sor_rec_preop rpp 
			INNER JOIN pre_premios p ON p.id = rpp.id_premio
			INNER JOIN sor_pgmsorteo ps ON ps.id = rpp.id_pgmsorteo AND ps.deleted = 0
			INNER JOIN pre_orden_pago opp ON opp.pre_premios_id_c = rpp.id_premio AND COALESCE(opp.`pre_premios_cuotas_id_c`,'') = '' AND opp.`deleted` = 0
			INNER JOIN age_permiso prm ON prm.id = p.age_permiso_id_c AND prm.deleted = 0
		WHERE rpp.cod_err = '0'
		;
	
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Proceso - paso 3 - FINALIZO INSERT pre_orden_pago_beneficiarios');	
			
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Proceso - paso 4.1 - update premios');
		
		/*
		 lmariotti 14-1-2026, porque si ya lo hace el PREMIOS_confirma_registro  para cada ID
		 
		UPDATE pre_premios pre
			INNER JOIN sor_rec_preop rp ON rp.id_premio = pre.id
		SET 	pre.pre_estadopago = 'A', 
			pre.date_modified = @hoy
		where rp.cod_err = '0'
		;
		*/

		OPEN mi_cursor_RR;
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - confirma PREMIO - inicio '));
		read_loop: LOOP	
			FETCH mi_cursor_RR INTO pid;

			SET @pid = pid; 	
			-- Si se terminó el cursor, salgo!
			IF done = 1 THEN		
				LEAVE read_loop; 
			END IF;     

			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), " CC_Premios_Pagados_OP_Proceso - CALL PREMIOS_confirma_registro('P', '", @pid,"');"));
			-- Mando una 'P' para decir que el canal es OTRAS PROVINCIAS, pero no quiero RECORDSET FINAL (MODO MUDO!!!)
			CALL PREMIOS_confirma_registro('P', @pid);
		END LOOP read_loop;
		CLOSE mi_cursor_RR;
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - confirma PREMIO - final '));	

		-- verifico si alguna confirmacion de premio falló para asignar el codigo de error correspondiente
		
		-- lmariotti 13-1-2025, control por dia, no puede procesar mas de una vez al dia !!!!!!!!!!!!!!!!!!!!!!!!!!!!
		-- no funciona esta modalidad
		
		IF (EXISTS(SELECT 'x' 
				FROM sor_rec_preop rec
				INNER JOIN sor_pgmsorteo pgm ON pgm.nrosorteo = rec.numero_sorteo AND pgm.idjuego = rec.numero_juego AND pgm.deleted = 0
				INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = pgm.id AND pre.pre_secuencia = rec.ocr AND pre.deleted = 0
				INNER JOIN pre_premios_confirma_registro_error er ON er.pre_premios_id_c = pre.id 
				WHERE DATE_FORMAT(er.fechaHoraProceso, '%Y%m%d') = DATE_FORMAT(NOW(), '%Y%m%d'))
		   ) THEN
			UPDATE sor_rec_preop rec
			INNER JOIN sor_pgmsorteo pgm ON pgm.nrosorteo = rec.numero_sorteo AND pgm.idjuego = rec.numero_juego AND pgm.deleted = 0
			INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = pgm.id AND pre.pre_secuencia = rec.ocr AND pre.deleted = 0
			INNER JOIN pre_premios_confirma_registro_error er ON er.pre_premios_id_c = pre.id 
			SET rec.cod_err = 200 + er.id_mensaje
			WHERE rec.cod_err = 0 AND DATE_FORMAT(er.fechaHoraProceso, '%Y%m%d') = DATE_FORMAT(NOW(), '%Y%m%d');
			SET cod_error = 200;
			SET mensaje = 'Ocurrió un problema durante la confirmación de los pagos. Consulte a Soporte.';
		END IF;

		-- Retorno
		SELECT cod_error, mensaje;
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - FIN OK - id_proceso: ', id_proceso, ' - opc: ', opc));  
		LEAVE fin;
		
	END IF;	
	
	IF (opc = 'OUT') THEN
		
		DROP TEMPORARY TABLE IF EXISTS tmp_registros_OP; 
		CREATE TEMPORARY TABLE IF NOT EXISTS tmp_registros_OP(
				secuencia INT(11) AUTO_INCREMENT,
				nom_archivo VARCHAR(255) DEFAULT NULL,
				nom_archivo_ctrl VARCHAR(255) DEFAULT NULL,
				contenido VARCHAR(1024) DEFAULT NULL,
				path_archivo VARCHAR(255) DEFAULT NULL,
			PRIMARY KEY(secuencia)
		);
		
					
		SET  @paq_path = (SELECT paq_path FROM exp_exportacion_tipo WHERE UPPER(TRIM(NAME)) = 'RESULTADO_PREMIOS_OTRAS_PROVINCIAS')
		;
		
		INSERT INTO tmp_registros_OP(nom_archivo,nom_archivo_ctrl,contenido,path_archivo)
			SELECT  CONCAT(SUBSTRING_INDEX(rctr.nombre_archivo, '.', 1),'.txt') AS nombre_archivo,
				'' AS nom_archivo_ctrl,
				CONCAT(
					LPAD(rec.provincia,2,'0'),LPAD(rec.numero_juego,3,'0'),LPAD(rec.numero_sorteo,6,'0'),LPAD(rec.ocr,10,'0'),LPAD(rec.digito_verificador,1,'0'),
					LPAD(REPLACE(rec.importe_premiado_neto,'.',''),17,'0'),LPAD(rec.fecha_pago,8,'0'),LPAD(rec.benef_tipo_cuit_afip,2,'0'),LPAD(rec.benef_cuit_cuil,11,'0'),
					RPAD(rec.benef_nombre,50,' '),RPAD(rec.benef_apellido,50,' '),RPAD(rec.benef_calle_nombre,50,' '),RPAD(rec.benef_calle_numero,6,' '),
					LPAD(rec.benef_cod_postal_correo,8,'0'),LPAD(rec.benef_cod_postal,4,'0'),RPAD(rec.benef_localidad,50,' '),LPAD(rec.benef_provincia,2,'0'),
					RPAD(rec.benef_email,50,' '),RPAD(rec.benef_sexo,1,' '),LPAD(rec.codigo_pais,3,'0'),LPAD(rec.num_doc_persona_human,11,'0'),
					RPAD(rec.tipo_benef,1,' '),RPAD(rec.benef_segundo_nombre,50,' '),LPAD(rec.benef_tipo_documento,1,'0'),RPAD(rec.benef_calle_piso,10,' '),
					RPAD(rec.benef_calle_depto,10,' '),RPAD(rec.benef_fecha_nac_ini_actividades,10,' '),RPAD(rec.benef_estado_civil,2,' '),RPAD(rec.benef_telef_carac,5,' '),
					RPAD(rec.benef_telef,9,' '),RPAD(rec.benef_pep,1,' '),RPAD(rec.benef_cargo,150,' '),LPAD(rec.benef_ocupacion,3,'0'),LPAD(rec.benef_nacionalidad,3,'0'),
					RPAD(rec.benef_tipo_sociedad,3,'0'),LPAD(rec.forma_pago,1,'0'),LPAD(rec.codigo_banco,3,'0'),LPAD(rec.tipo_cuenta,1,'0'),LPAD(rec.numero_cuenta,11,'0'),
					RPAD(rec.cbu,22,' '),LPAD(rec.numero_cheque,11,'0'),RPAD(rec.cod_err,5,' ')
					) AS contenido,
				CONCAT(@paq_path,rctr.secuencia_envio) AS path_archivo
			FROM sor_rec_preop rec 
			INNER JOIN sor_rec_preop_ctr rctr ON rctr.id_proceso = rec.id_proceso
			WHERE rec.cod_err != '0'
		;
		
		SET @msgRet = 'OK';
		SET @msgaux = 'contenido de archivo' ;

		-- Retorno
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - FIN OK - id_proceso: ', id_proceso, ' - opc: ', opc));  

		SELECT 	top.secuencia AS secuencia,
			nom_archivo AS nombre_archivo, 
			'' AS nombre_archivo_ctrl,
			top.contenido AS contenido, 
			top.path_archivo AS ruta, 
			@msgRet AS mensaje,
			cod_error 
		FROM tmp_registros_OP top;
		LEAVE fin;
	END IF;	

	IF (opc = 'UPD') THEN
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - INI - id_proceso: ', id_proceso, ' - opc: ', opc));  
		
		-- primero actualizo totales de la emisión
		CALL PREMIOS_CIERRE_recalcula_resumen_emision((SELECT ps.id FROM sor_rec_preop pop INNER JOIN sor_pgmsorteo ps ON ps.idjuego = pop.numero_juego AND ps.nrosorteo = pop.numero_sorteo AND ps.deleted = 0),(SELECT id_proceso FROM sor_rec_preop_ctr), @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		
		-- obtengo datos para actualizar tabla de secuencia
		SET v_sec = NULL, v_prv = NULL;
		SELECT DISTINCT c.secuencia_envio, c.provincia INTO v_sec, v_prv
		FROM sor_rec_preop_ctr c 
		WHERE c.id_proceso = id_proceso
		;
		-- si hay datos actualizo tabla de secuencia
		IF ((v_sec IS NOT NULL) AND (v_prv IS NOT NULL)) THEN
			IF (!actualiza_sgte_secuencia_prepag_op(v_prv, v_sec)) THEN 
				SET cod_error = 999, mensaje = 'Se produjo un problema al actualizar la secuencia de envío. Consulte a Soporte.';
				INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - FIN ERR - id_proceso: ', id_proceso, ' - opc: ', opc, ' - cod_error: ', cod_error, ' - mensaje: ', mensaje));  
				
			ELSE
				INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - FIN OK - id_proceso: ', id_proceso, ' - opc: ', opc));  
			END IF;
		ELSE
			SET cod_error = 999, mensaje = 'Se produjo un problema al actualizar la secuencia de envío. Consulte a Soporte.';
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' CC_Premios_Pagados_OP_Proceso - FIN ERR - id_proceso: ', id_proceso, ' - opc: ', opc, ' - cod_error: ', cod_error, ' - mensaje: ', mensaje));  
		END IF;
		SELECT cod_error, mensaje;
		LEAVE fin;			
	END IF;	

	  
END$$

DELIMITER ;