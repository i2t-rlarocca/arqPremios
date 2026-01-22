DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `CC_Premios_Pagados_OP_Validacion`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `CC_Premios_Pagados_OP_Validacion`(
 IN id_proceso INT,
 IN id_archivo INT,
 IN usuario VARCHAR(255),
 IN loteria INT,
 IN tipo_validacion CHAR(3),
 OUT juego_s VARCHAR(2),
 OUT sorteo_s VARCHAR(8),
 OUT ticket_s VARCHAR(10),
 OUT provincia_s VARCHAR(2),
 OUT agencia_s VARCHAR(5),
 OUT importe_s VARCHAR(13),
 OUT cod_error VARCHAR(5)
)
fin:BEGIN
	DECLARE valida_nro_envio INT(10);
	DECLARE juego INT(2);
	DECLARE importe VARCHAR(13);
	DECLARE c_sorteo INT(8);
	DECLARE ticket INT(10);
	DECLARE provincia VARCHAR(2);
	DECLARE permiso INT(6);
	DECLARE agencia VARCHAR(5);
	DECLARE subagencia VARCHAR(5);
	DECLARE account_id VARCHAR(36);
	DECLARE resultado INT DEFAULT 0; -- Para contar los resultados del select
	DECLARE resultado2 INT DEFAULT 0;
	DECLARE id_premio VARCHAR(36);
	DECLARE id_premio_menores VARCHAR(36);
	DECLARE n INT DEFAULT 0;
	DECLARE i INT DEFAULT 0;
	DECLARE done INT DEFAULT 0;
	DECLARE par_juego INT DEFAULT 99;
	DECLARE par_sorteo INT;
	DECLARE par_idEstado INT DEFAULT 50;
	DECLARE par_idEstado_final INT DEFAULT 50;
	DECLARE par_evento INT DEFAULT 92;
	DECLARE mensaje VARCHAR(255) DEFAULT '';
	-- DECLARE cod_error VARCHAR(50);
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	SET cod_error = '0';
	SET valida_nro_envio = 0;
	SET @id_archivo = id_archivo;	
	SET @usuario = usuario;  
	SET @id_proceso = id_proceso;
	SET par_juego = 99;
	SET par_sorteo = @id_archivo;
	SET par_idEstado = 50;
	SET par_idEstado_final = 50;
	SET mensaje = 'OK';
	SET @continuar = 1;
	SET @fecha_act = DATE_FORMAT(CURDATE(), '%Y%m%d');
	SET @v_fecha_minima = '';
	
	SET @secuencia_enviada=0;	
	SELECT secuencia_envio
		INTO @secuencia_enviada		
	FROM sor_rec_preop_ctr
	;
	IF (tipo_validacion = 'SEC') THEN
	    INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - comienzo valida SEC');  
		SET @secuencia_esp=0;
		
		SELECT obtiene_sgte_secuencia_prepag_op(loteria) 
			INTO @secuencia_esp;
		
		IF (@secuencia_enviada != @secuencia_esp) THEN
		
			SET cod_error = '555';
			-- SET mensaje = CONCAT('COD-555 Problema validación de secuencia ESPERADA');
			SET mensaje = CONCAT('COD-000 Problema validación de secuencia, secuencia ', @secuencia_enviada, " diferente a la esperada (",@secuencia_esp,").");
			SELECT cod_error,mensaje;
			
			/* Se trajo ejemplo de sp CC_Valida_CtaCte_Agencia - RR
			SET @cod_err = '1';
			SET msgret=CONCAT("Problema validación de secuencia, secuencia ", @secuencia_enviada, " diferente a la esperada (",@secuencia_esp,").");
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err,' - ', msgret);			
			CALL sor_inserta_auditoria(@id_proceso,-999, @id_secuencia,27,@usuario,26,28, @msgaud);*/
			LEAVE fin;
		END IF;		
	
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - fin valida SEC');
		SELECT cod_error,mensaje;
		LEAVE fin;
	
	END IF;
      
	IF (tipo_validacion = 'ENV') THEN
	   INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - comienzo ENV');  
		-- Valida archivo de datos
		SET @cantidad_dat = 0;
		SET @suma_importes_dat = 0;
		SET @provincia_dat = -1;
		SELECT  provincia,
			COUNT(*),
			SUM(importe_premiado_neto) 
		INTO @provincia_dat, @cantidad_dat, @suma_importes_dat
		FROM sor_rec_preop
		GROUP BY provincia
		LIMIT 1
		;
		-- Vinieron registros?
		IF (@cantidad_dat = 0) THEN
			SET cod_error = '002';
			SET mensaje = CONCAT('COD-002 No se recibió el archivo de datos, secuencia ', @secuencia_enviada);
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
		-- Pertenecen a la lotería en proceso?
		IF (@provincia_dat != loteria) THEN
			SET cod_error = '002';
			SET mensaje = CONCAT('COD-002 Los datos en el archivo de datos no pertenecen a la lotería en proceso, secuencia ', @secuencia_enviada);
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
		
		-- Valida archivo de control
		SET @contador_ctr = 0;
		SET @importes_ctr = 0;
		SET @provincia_ctr = 0;
		SELECT COUNT(*)
			INTO @contador_ctr
		FROM sor_rec_preop_ctr
		;
		-- Vinieron registros?
		IF (@contador_ctr = 0) THEN
			SET cod_error = '003';
			SET mensaje = CONCAT('COD-003 No se recibió el archivo de control.');
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;
		END IF;
		-- Vino exactamente 1 registro?
		IF (@contador_ctr > 1) THEN
			SET cod_error = '003';
			SET mensaje = CONCAT('COD-003 El archivo de control trajo más de un registro.');
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;
		END IF;
	
		SET @md5 = 0;
		SET @cantidad_ctr = 0;
		SET @importes_ctr = 0;
		
		SELECT CASE WHEN md5_archivo = md5_calculado THEN 0 ELSE 1 END, cantidad_registros, suma_importe, provincia
		INTO @md5, @cantidad_ctr, @importes_ctr, @provincia_ctr
		FROM sor_rec_preop_ctr
		;
		-- MD5 correcto?
		-- SET @md5 = 0; -- Asterisquear en produccion!!!!
		IF (@md5 = 1) THEN
			SET cod_error = '004';
			SET mensaje = CONCAT('COD-004 La firma md5 del archivo de datos no coincide con la enviada.');
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
		-- Pertenecen a la lotería en proceso?
		IF (@provincia_ctr != loteria) THEN
			SET cod_error = '002';
			SET mensaje = CONCAT('COD-002 Los datos en el archivo de conntrol no pertenecen a la lotería en proceso, secuencia ', @secuencia_enviada);
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
		-- Cantidad de registros de datos coincide con cant informada?
		IF (@cantidad_ctr != @cantidad_dat) THEN
			SET cod_error = '005';
			SET mensaje = CONCAT('COD-005 No coincide la cantidad de registros recibida con la indicada en el archivo de control.');
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
		-- Importes correctos y coinciden?
		IF (@importes_ctr != @suma_importes_dat OR COALESCE(@importes_ctr,0) <= 0 OR COALESCE(@suma_importes_dat,0) <= 0) THEN
			SET cod_error = '006';
			SET mensaje = CONCAT('COD-006 No coincide el importe total recibido con la indicada en el archivo de control.');
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
		-- Hay registros de otro provincia?
		IF (SELECT COUNT(provincia) FROM sor_rec_preop WHERE provincia != loteria) > 0 THEN
			SET cod_error = '007';
			SET mensaje = CONCAT('COD-007 Existen registros que no corresponden a la provincia indicada en el archivo de control.');
			SELECT cod_error,mensaje; -- select de depuracion manual
			-- CALL sor_inserta_auditoria(id_proceso,par_juego,par_sorteo,par_evento,usuario,par_idEstado,par_idEstado_final,mensaje);
			LEAVE fin;			
		END IF;
	
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - fin valida ENV');
		SELECT cod_error,mensaje;
		LEAVE fin;
	
	END IF;
	
	IF (tipo_validacion = 'REG') THEN
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - comienzo valida REG');
		
		-- terminales
		UPDATE sor_rec_preop rec 
		LEFT JOIN sor_pgmsorteo pgm ON pgm.nrosorteo = rec.numero_sorteo AND pgm.idjuego = rec.numero_juego AND pgm.deleted = 0
		LEFT JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = pgm.id AND pre.pre_secuencia = rec.ocr AND pre.deleted = 0
		LEFT JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		SET rec.cod_err = CASE 	WHEN (pgm.id IS NULL) THEN '051' -- juego/sorteo no existe
					WHEN (pre.id IS NULL) THEN '052' -- premio no existe
					WHEN (pre.id IS NOT NULL AND rec.provincia != pro.prv_id_boldt) THEN '053' -- premio si existe pero otra provincia
					WHEN (pre.id IS NOT NULL AND rec.importe_premiado_neto != pre.pre_impneto) THEN '054' -- total neto no coincide
					WHEN (pre.pre_estinfuif != 'N' AND LENGTH(rec.num_doc_persona_human) < 2 ) THEN '059' -- es uif (aplica) y no viene doc de persona dato fundamental para 
					ELSE rec.cod_err END
		WHERE rec.cod_err = '0'
		;
		
		-- informativos
		UPDATE sor_rec_preop rec
		LEFT JOIN sor_pgmsorteo pgm ON pgm.nrosorteo = rec.numero_sorteo AND pgm.idjuego = rec.numero_juego AND pgm.deleted = 0
		LEFT JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = pgm.id AND pre.pre_secuencia = rec.ocr AND pre.deleted = 0
		LEFT JOIN pre_orden_pago op ON op.pre_premios_id_c = pre.id AND op.deleted = 0
		LEFT JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = op.id AND opb.deleted = 0
		SET rec.cod_err = CASE 	WHEN ((pre.id IS NOT NULL AND op.id IS NOT NULL AND opb.id IS NOT NULL) 
						AND pre.pre_estadopago = 'A' 
						AND DATE_FORMAT(op.opp_fecha_pago,'%d-%m-%Y') =  DATE_FORMAT(rec.fecha_pago,'%d-%m-%Y')
						AND rec.benef_cuit_cuil = opb.opb_cuit) 
						THEN '055' -- pagado 
					WHEN ((pre.id IS NOT NULL AND op.id IS NOT NULL AND opb.id IS NOT NULL) 
						AND pre.pre_estadopago = 'A' 
						AND DATE_FORMAT(op.opp_fecha_pago,'%d-%m-%Y') =  DATE_FORMAT(rec.fecha_pago,'%d-%m-%Y')
						AND rec.benef_cuit_cuil != opb.opb_cuit) 
						THEN '056' -- pagado no coincd cuit
					WHEN ((pre.id IS NOT NULL AND op.id IS NOT NULL AND opb.id IS NOT NULL) 
						AND pre.pre_estadopago = 'A' 
						AND DATE_FORMAT(op.opp_fecha_pago,'%d-%m-%Y') !=  DATE_FORMAT(rec.fecha_pago,'%d-%m-%Y')
						AND rec.benef_cuit_cuil = opb.opb_cuit) 
						THEN '057' -- pagado no coincd fecha pago 
					WHEN ((pre.id IS NOT NULL AND op.id IS NOT NULL AND opb.id IS NOT NULL) 
						AND pre.pre_estadopago = 'A' 
						AND DATE_FORMAT(op.opp_fecha_pago,'%d-%m-%Y') !=  DATE_FORMAT(rec.fecha_pago,'%d-%m-%Y')
						AND rec.benef_cuit_cuil != opb.opb_cuit) 
						THEN '058' -- pagado no coincd fecha pago ni cuit
					ELSE rec.cod_err END
		WHERE rec.cod_err = '0';
	
		-- terminales a partir de 070
		UPDATE sor_rec_preop rec
		LEFT JOIN tbl_provincias pro ON pro.prv_id_boldt = rec.benef_provincia AND pro.deleted = 0
		LEFT JOIN tbl_localidades loc ON loc.loc_cpos = rec.benef_cod_postal AND loc.loc_scpo = rec.benef_cod_postal_correo AND loc.deleted = 0 -- loc.billing_address_city = UPPER(TRIM(rec.benef_localidad)) -- ??? agrego descripcion de localidad ?   loc.billing_address_city = UPPER(TRIM(rec.benef_localidad))
		LEFT JOIN tbl_paises pai ON pai.id = CONVERT(rec.codigo_pais, CHAR(3)) AND pai.deleted = 0 -- RR-3
		-- LEFT JOIN tbl_paises pai ON (pai.id = CONVERT(rec.codigo_pais, CHAR(3)) OR pai.name = UPPER(rec.pais_desc)) AND pai.deleted = 0 -- RR-2		
		-- LEFT JOIN tbl_paises pai ON (pai.id = rec.codigo_pais OR pai.name = UPPER(rec.pais_desc)) AND pai.deleted = 0 -- RR-1

		SET rec.cod_err = CASE 	WHEN (LENGTH(rec.benef_cuit_cuil) < 2 OR rec.benef_cuit_cuil IS NULL OR rec.benef_cuit_cuil = '') THEN '070' -- cuit no informado
					WHEN (LENGTH(rec.benef_cuit_cuil) != 11 ) THEN '071' -- hay que agregar mayor validacion, debe estar en otro lugar
					WHEN COALESCE(rec.fecha_pago, 20250101) <= 20250101 THEN '072' -- fecha pago no indicada		   									
					WHEN pro.id IS NULL THEN '074' -- provincia indicada no existe
					WHEN loc.id IS NULL THEN '075' -- localidad no existe
					WHEN pai.id IS NULL THEN '076' -- pais no existe
				ELSE rec.cod_err END 
		WHERE rec.cod_err = '0';
		-- inserto las localidades no encontradas que no son de santa fe
		INSERT INTO tbl_localidades 
		SELECT CONCAT(r.benef_cod_postal,'-',r.benef_cod_postal_correo) AS id,
			TRIM(r.benef_localidad) AS NAME, 
			NOW(),NOW(),'1','1',
			CONCAT('Localidad ',TRIM(r.benef_localidad)) AS description, 0, '1', 
			TRIM(r.benef_localidad) AS billing_address_city,
			r.benef_cod_postal AS loc_cpos,
			r.benef_cod_postal_correo AS loc_scpo,
			p.id AS tbl_provincias_id_c,
			r.codigo_pais AS tbl_paises_id_c,
			'N' AS loc_zona_riesgo, CONCAT(p.id,'000') AS tbl_departamentos_id_c,
			0 AS ruta_pac, 0 AS cant_hab_2010, CONCAT(p.id,'00') AS tbl_nodos_id_c, 0 AS cant_hab, 'Poblacion' AS categoria, 0 AS ruta_pac_orden,
			NULL AS codigo_provincia
		FROM sor_rec_preop r
		INNER JOIN tbl_provincias p ON r.benef_provincia = p.prv_id_boldt
		INNER JOIN tbl_paises pa ON pa.id = p.tbl_paises_id_c AND pa.deleted = 0
		WHERE p.deleted = 0 AND p.prv_id_boldt NOT IN (3)
		AND pa.id = CONVERT(r.codigo_pais, CHAR(3))  
		AND r.cod_err = '075'
		;
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - SELECT CIERRE ADM');
		-- VALIDACION DE FECHA CIERRE ADM			
		SELECT v.vto_fecha_desde INTO @v_fecha_minima
			FROM imp_vencimientos v
			JOIN (	SELECT v1.imp_impuesto, MIN(v1.vto_fecha_cierre_adm) AS max_ca
				FROM imp_vencimientos v1
				WHERE v1.imp_impuesto = 'SIC'
				AND v1.vto_fecha_cierre_adm >= CURDATE()
				GROUP BY v1.imp_impuesto) va 
			ON va.imp_impuesto = v.imp_impuesto AND va.max_ca = v.vto_fecha_cierre_adm;
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - FIN SELECT CIERRE ADM');

		IF @v_fecha_minima IS NULL OR @v_fecha_minima = '' THEN 
		  SET @v_fecha_minima = '9999-99-99';
		END IF;

		UPDATE sor_rec_preop rec	
		-- lmariotti 12-1-2025, es menor difiere del control online
		-- SET rec.cod_err = CASE WHEN rec.fecha_pago > DATE_FORMAT(@v_fecha_minima,'%Y%m%d') THEN '073'	-- fecha pago no esta incl. en las quincenas??? de pres a afip beetwen	
		SET rec.cod_err = CASE WHEN rec.fecha_pago < DATE_FORMAT(@v_fecha_minima,'%Y%m%d') THEN '073'	-- fecha pago no esta incl. en las quincenas??? de pres a afip beetwen
				  ELSE rec.cod_err END						
		WHERE rec.cod_err = '0';		
		
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - Fecha minima', @v_fecha_minima );
		
		-- UIF???  terminales 
		UPDATE sor_rec_preop rec
		LEFT JOIN tbl_listas_desplegables ldp ON ldp.codigo = UPPER(rec.tipo_benef) AND ldp.name = 'tipo_persona_list' AND ldp.deleted = 0
		LEFT JOIN tbl_listas_desplegables ldp2 ON ldp2.codigo = rec.benef_tipo_cuit_afip AND ldp2.name = 'tipo_documental_afip_list' AND ldp2.deleted = 0
		SET rec.cod_err = CASE 	WHEN ldp.id IS NULL THEN '077' -- tipo persona no existe
					WHEN (rec.tipo_benef = 'J' AND SUBSTR(rec.benef_cuit_cuil,1,2) NOT IN ('30','33','34')) 
						OR (rec.tipo_benef = 'F' AND SUBSTR(rec.benef_cuit_cuil,1,2) NOT IN ('20','23','24','27'))
						THEN '078' -- tipo persona no coincide con el cuit 
					WHEN (rec.benef_tipo_cuit_afip = '' OR rec.benef_tipo_cuit_afip = 0 OR rec.benef_tipo_cuit_afip = NULL) THEN '079'
					WHEN ldp2.id IS NULL THEN '080' -- no se ha infomado tipo cuit \ documento?
				ELSE rec.cod_err END
		WHERE (rec.codigo_pais != '' AND rec.codigo_pais != '0') -- estos datos deberian venir si es uif
		AND (rec.tipo_benef != '' AND rec.tipo_benef != '0') -- estos datos deberian venir si es uif
		AND rec.cod_err = '0';
	
	
		-- RR terminales valida nombre, apellido, domicilio, numero, cod. postal, localidad 
		UPDATE sor_rec_preop rec			
			SET rec.cod_err = CASE 	WHEN rec.benef_nombre = '' OR rec.benef_apellido = '' OR rec.benef_nombre = NULL OR rec.benef_apellido = NULL THEN '102' 
						WHEN rec.benef_calle_nombre = '' OR rec.benef_calle_numero = '' OR rec.benef_calle_nombre = NULL OR rec.benef_calle_numero = NULL THEN '103' 
						WHEN rec.benef_cod_postal = '' OR rec.benef_cod_postal = NULL OR rec.benef_localidad = '' OR rec.benef_localidad = NULL THEN '104' -- este error salta con cod 075					
						ELSE rec.cod_err END
			WHERE rec.cod_err = '0';-- FIN RR	
		
		/* RL: premios uif otras provincias no se informan a Santa Fe
		-- UIF y persona fisica
		UPDATE sor_rec_preop rec
			LEFT JOIN tbl_listas_desplegables ldp3 ON ldp3.codigo = UPPER(rec.benef_tipo_documento) AND ldp3.name = 'tipo_documento_list' AND ldp3.deleted = 0
			SET rec.cod_err = CASE 	WHEN rec.tipo_benef = 'F' AND (rec.benef_tipo_documento = '' OR rec.num_doc_persona_human = '') THEN '111' 
						WHEN rec.tipo_benef = 'F' AND ldp3.id IS NULL THEN '112' -- tipo docu no existe
						-- WHEN (rec.tipo_benef = 'F' AND (CAST(rec.num_doc_persona_human AS CHAR) != SUBSTR(rec.benef_cuit_cuil,3,8))) THEN '113'
						WHEN (rec.tipo_benef = 'F' AND (SUBSTR(rec.num_doc_persona_human,3,8) != SUBSTR(rec.benef_cuit_cuil,3,8))) THEN '113'
						WHEN (rec.tipo_benef = 'F' AND (rec.benef_fecha_nac_ini_actividades = '' OR rec.benef_fecha_nac_ini_actividades = NULL)) THEN '114'
						WHEN (rec.tipo_benef = 'F' AND (DATEDIFF(CURDATE(),DATE_ADD(DATE_FORMAT(rec.benef_fecha_nac_ini_actividades, '%Y-%m-%d'), INTERVAL 18 YEAR)) >= 0 AND
										DATEDIFF(DATE_ADD(DATE_FORMAT(rec.benef_fecha_nac_ini_actividades, '%Y-%m-%d'), INTERVAL 110 YEAR),CURDATE()) < 0)
							) THEN '115'
						ELSE rec.cod_err END
			WHERE rec.cod_err = '0'
			AND rec.tipo_benef = 'F'
			AND (rec.codigo_pais != '' AND rec.codigo_pais != '0') -- estos datos deberian venir si es uif
			AND (rec.tipo_benef != '' AND rec.tipo_benef != '0') -- estos datos deberian venir si es uif
			;
		-- UIF y persona fisica
		UPDATE sor_rec_preop rec 
			LEFT JOIN tbl_listas_desplegables ldp4 ON ldp4.codigo = rec.benef_estado_civil AND ldp4.name = 'estado_civil_list' AND ldp4.deleted = 0 
			LEFT JOIN tbl_ocupaciones ocu ON ocu.codigo = rec.benef_ocupacion AND ocu.estado = 'activo' AND ocu.deleted = 0
			LEFT JOIN tbl_nacionalidades nac ON nac.id = rec.benef_nacionalidad AND nac.deleted = 0
			-- LEFT JOIN tbl_paises pai ON pai.id = rec.benef_nacionalidad AND pai.deleted = 0
			SET rec.cod_err = CASE 
						WHEN rec.tipo_benef = 'F' AND (rec.benef_pep = '' OR rec.benef_pep = NULL OR rec.benef_pep NOT IN ('S','N')) THEN '116' -- RR
						-- WHEN rec.benef_pep = '' OR rec.benef_pep = NULL OR rec.benef_pep NOT IN ('S','N') THEN '116' -- tipo docu no existe RR
						WHEN rec.benef_pep IN ('S','N') AND (rec.benef_cargo = '' OR rec.benef_cargo = NULL) THEN '117'
						WHEN (rec.benef_estado_civil = '' OR rec.benef_estado_civil = NULL) THEN  '121'
						WHEN  ldp4.id IS NULL THEN '122'						
						-- WHEN (rec.benef_ocupacion = '' OR rec.benef_ocupacion = NULL) THEN '123'
						WHEN rec.tipo_benef = 'F' AND (rec.benef_ocupacion = '' OR rec.benef_ocupacion = NULL) THEN '123'
						-- WHEN ocu.id IS NULL THEN '124' RR
						WHEN rec.tipo_benef = 'F' AND (ocu.id IS NULL) THEN '124'
						WHEN (rec.benef_nacionalidad = '' OR rec.benef_nacionalidad = NULL OR rec.benef_nacionalidad = 0) THEN '125'
						WHEN nac.id IS NULL THEN '126'
						ELSE rec.cod_err END
			WHERE rec.cod_err = 0 
			-- AND rec.tipo_benef = 'F' -- filtra por persona fisica... pero quizas va como and en cada when
			AND (rec.codigo_pais != '' AND rec.codigo_pais != '0') -- estos datos deberian venir si es uif
			AND (rec.tipo_benef != '' AND rec.tipo_benef != '0') -- estos datos deberian venir si es uif
			;
			
		-- UIF y persona juridica
		UPDATE sor_rec_preop rec
		LEFT JOIN tbl_listas_desplegables ldp5 ON ldp5.codigo = rec.benef_tipo_sociedad AND ldp5.name = 'uif_tipo_sociedad' AND ldp5.deleted = 0
		LEFT JOIN tbl_ocupaciones ocu ON ocu.codigo = rec.benef_ocupacion AND ocu.estado = 'activo' AND ocu.deleted = 0
		SET rec.cod_err = CASE 
					WHEN rec.tipo_benef = 'J' AND (rec.benef_pep != '' OR rec.benef_pep != NULL) THEN '131' -- tipo docu no existe
					WHEN rec.tipo_benef = 'J' AND (rec.benef_fecha_nac_ini_actividades = '' OR rec.benef_fecha_nac_ini_actividades != NULL) THEN '132'
					-- WHEN rec.tipo_benef = 'J' AND rec.benef_ocupacion THEN  '133' -- no se indica actividad principal RR
					WHEN rec.tipo_benef = 'J' AND (rec.benef_ocupacion = '' OR rec.benef_ocupacion = NULL) THEN  '133' -- no se indica actividad principal -- RR				
					WHEN rec.tipo_benef = 'J' AND ocu.id IS NULL THEN  '134' -- no se indica actividad principal
					WHEN rec.tipo_benef = 'J' AND (rec.benef_tipo_sociedad = NULL OR rec.benef_tipo_sociedad = '') THEN '135'
					WHEN rec.tipo_benef = 'J' AND ldp5.id IS NULL THEN '136'
					ELSE rec.cod_err END
		WHERE rec.cod_err = 0 
		AND (rec.codigo_pais != '' AND rec.codigo_pais != '0') -- estos datos deberian venir si es uif
		AND (rec.tipo_benef != '' AND rec.tipo_benef != '0') -- estos datos deberian venir si es uif
		;	
		
		
		-- formas de pago
		UPDATE sor_rec_preop rec
		LEFT JOIN tbl_listas_desplegables ldp6 ON ldp6.codigo = rec.forma_pago AND ldp6.name = 'forma_pago_list' AND ldp6.deleted = 0
		LEFT JOIN tbl_listas_desplegables ldp7 ON ldp7.codigo = rec.tipo_cuenta AND ldp7.name = 'tipo_cuenta_list' AND ldp7.deleted = 0
		LEFT JOIN tb2_bancos bco ON bco.bco_identificador = rec.codigo_banco AND bco.bco_estado = 'activo' AND bco.deleted = 0
		SET rec.cod_err = CASE 
					WHEN (rec.forma_pago = '' OR rec.forma_pago = NULL OR rec.forma_pago = 0) THEN '151' 
					WHEN ldp6.id IS NULL THEN '152' 
					WHEN rec.forma_pago IN (2,3) AND (rec.codigo_banco = NULL OR rec.codigo_banco = 0 OR rec.codigo_banco = '') THEN  '153' 
					WHEN rec.forma_pago IN (2,3) AND bco.id IS NULL THEN '154'
					WHEN rec.forma_pago IN (2,3) AND (rec.tipo_cuenta = NULL OR rec.tipo_cuenta = 0 OR rec.tipo_cuenta = '') THEN  '155' 
					WHEN rec.forma_pago IN (2,3) AND ldp7.id IS NULL THEN '156'
					WHEN rec.forma_pago IN (2,3) AND (rec.numero_cuenta = NULL OR rec.numero_cuenta = 0 OR rec.numero_cuenta = '') THEN  '157' 
					WHEN rec.forma_pago = 2 AND (rec.cbu = NULL OR rec.cbu = 0 OR rec.cbu = '') THEN  '158' 
					WHEN rec.forma_pago = 3 AND (rec.numero_cheque = NULL OR rec.numero_cheque = 0 OR rec.numero_cheque = '') THEN  '159' 
					ELSE rec.cod_err END
		WHERE rec.cod_err = 0 
		AND (rec.codigo_pais != '' AND rec.codigo_pais != '0') -- estos datos deberian venir si es uif
		AND (rec.tipo_benef != '' AND rec.tipo_benef != '0') -- estos datos deberian venir si es uif
		; 
		RL: premios uif otras provincias no se informan a Santa Fe */
	
		-- RR terminales valida FECHA DE PAGO
		UPDATE sor_rec_preop rec		
			LEFT JOIN sor_pgmsorteo pgm ON pgm.nrosorteo = rec.numero_sorteo AND pgm.idjuego = rec.numero_juego AND pgm.deleted = 0 
			SET rec.cod_err = CASE					
						WHEN rec.fecha_pago < DATE_FORMAT(pgm.fecha, '%Y%m%d') THEN '160' -- sisJava cod32 	
						WHEN rec.fecha_pago > @fecha_act THEN '161' -- sisJava cod33						
					ELSE rec.cod_err END
			WHERE rec.cod_err = '0';

			
		-- aqui podría hacer update de todos los id de tablas relacionadas. donde el cod_err = 0
		UPDATE sor_rec_preop rec
			INNER JOIN sor_pgmsorteo pgm ON pgm.nrosorteo = rec.numero_sorteo AND pgm.idjuego = rec.numero_juego AND pgm.deleted = 0
			INNER JOIN tbl_provincias prv ON prv.prv_id_boldt = rec.provincia AND prv.deleted = 0
			INNER JOIN tbl_localidades loc ON loc.loc_cpos = rec.benef_cod_postal 
				AND rec.benef_cod_postal_correo = loc.loc_scpo
				-- AND loc.billing_address_city = UPPER(TRIM(rec.benef_localidad)) 
				AND loc.deleted = 0
				-- lmariotti 14-1-2026, no hace join porque no considera subcodigopostal
				
			INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = pgm.id AND pre.tbl_provincias_id_c = prv.id 
							AND pre.pre_secuencia = rec.ocr AND pre.deleted = 0
			-- LEFT JOIN tbl_nacionalidades nac ON nac.tbl_paises_id_c = rec.benef_nacionalidad AND nac.deleted = 0
			LEFT JOIN tbl_nacionalidades nac ON nac.tbl_paises_id_c = CONVERT(rec.benef_nacionalidad, CHAR(3)) AND nac.deleted = 0
			LEFT JOIN tbl_paises pai ON (pai.id = CAST(rec.codigo_pais AS CHAR)) AND pai.deleted = 0 -- RR
			-- LEFT JOIN tbl_paises pai ON (pai.name = UPPER(TRIM(rec.pais_desc)) COLLATE utf8_spanish_ci) OR pai.id = CAST(rec.codigo_pais AS CHAR) AND pai.deleted = 0 --RR
			LEFT JOIN tbl_ocupaciones ocu ON ocu.codigo = rec.benef_ocupacion AND ocu.deleted = 0
		SET 	rec.id_pgmsorteo = COALESCE(pgm.id,''),
			rec.id_premio = COALESCE(pre.id,''),
			rec.id_provincia = COALESCE(prv.id,''),
			rec.id_localidad = COALESCE(loc.id,''),
			rec.id_ocupacion = COALESCE(ocu.id,''),
			rec.id_pais = COALESCE(pai.id,''),
			rec.id_nacionalidad = COALESCE(nac.id,'')
		WHERE rec.cod_err = '0' AND pre_clasepremio= 'B'
		;
		INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_Premios_Pagados_OP_Validacion - fin valida REG');
				
		SELECT cod_error,mensaje;
		LEAVE fin;
		
	END IF;
	
END$$

DELIMITER ;