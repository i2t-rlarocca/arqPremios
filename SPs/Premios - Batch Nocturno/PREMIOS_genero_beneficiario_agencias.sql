DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_genero_beneficiario_agencias`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_genero_beneficiario_agencias`(IN user_id CHAR(36),OUT msgret VARCHAR(2048))
    COMMENT 'Genero BENEFICIARIOS/LIQUIDACION para premios con RETENCION que no requieren envio a UIF y no fueron informados'
fin :BEGIN
	DECLARE var_tbl_ocupaciones_id_c CHAR(36);
	DECLARE var_tbl_paises_id_c CHAR(36);
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1
			@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
		 	@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
		IF (@trans = 1) THEN
			ROLLBACK;
		END IF;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - FIN CON ERROR - etapa ', @etapa, 
				' - code:', @code, ' - errno:', @errno, ' - msg:', @msg));
		SET msgret=CONCAT("Problemas SP PREMIOS_genero_beneficiario_agencias - etapa: ", @etapa, ' - code:', @code, ' - errno:', @errno, ' - msg:', @msg);
	END;
	-- Diario elimino las sessiones anteriores... AE - 19/1/2018
	IF (HOUR(NOW()) < 7) THEN
		DELETE r FROM pre_sesiones_www r WHERE r.fecha_hora_sesion < CURDATE();
	END IF;
	-- No proceso los días domingo!
	IF (UPPER(DATE_FORMAT(NOW(),'%a')) = 'SUN') THEN
		SET msgret = CONCAT('Los domingos no procesa...');
		LEAVE fin ;
	
	END IF;
	-- Días 2, 3, 17 y 18 lo llevo a 2 días y 1 adicional para acelerar el cambio de quincena...
	IF (DAY(CURDATE()) = 2 OR DAY(CURDATE()) = 3 OR DAY(CURDATE()) = 4
		OR DAY(CURDATE()) = 17 OR DAY(CURDATE()) = 18 OR DAY(CURDATE()) = 19) THEN
		SET @var_santafe = 'S', @dias_gracia = 3, @dias_gracia_adic = 0, @ahora = NOW(), @etapa = 0;
	ELSE
		SET @var_santafe = 'S', @dias_gracia = 3, @dias_gracia_adic = 2, @ahora = NOW(), @etapa = 0;
	END IF;
	-- Recupero el ID de la ocupacion del permisionario...
	SET var_tbl_ocupaciones_id_c  = '', @etapa = @etapa + 1;
	SELECT id INTO var_tbl_ocupaciones_id_c 
		FROM tbl_ocupaciones o 
		WHERE o.`name` = 'PERMISIONARIO';
		
	-- Recupero el ID del pais por defecto...
	SET var_tbl_paises_id_c  = '', @etapa = @etapa + 1;
	SELECT id INTO var_tbl_paises_id_c
		FROM tbl_paises p 
		WHERE p.`name` = 'ARGENTINA';
	IF (var_tbl_ocupaciones_id_c = '' OR var_tbl_paises_id_c = '') THEN
		SET msgret = CONCAT('Verifique tabla de ocupaciones -PERMISIONARIO-', var_tbl_ocupaciones_id_c, 
					' y de paises -ARGENTINA-', var_tbl_paises_id_c);
		LEAVE fin ;
	END IF;
	-- Si ya tiene pago, tiene ORDEN DE PAGO PROVISORIA, hay que actualizarla!
	-- Determino los premios a actualizar!
	DROP TEMPORARY TABLE IF EXISTS tmp_pagos_a_informar;
	CREATE TEMPORARY TABLE tmp_pagos_a_informar
		SELECT p.id AS idPremio, p.`pre_secuencia`, p.`pre_impbruto`, p.`pre_impneto`, 
			opp.id AS idOpago, opp.`opp_fecha_pago`, p.`age_permiso_id_c`,
			ps.`idjuego`, ps.`nrosorteo`
 
			FROM pre_premios p
				JOIN sor_pgmsorteo ps ON ps.id = p.sor_pgmsorteo_id_c AND ps.deleted = 0
				JOIN sor_producto prd ON prd.id = ps.`sor_producto_id_c` AND prd.`deleted` = 0
				JOIN tbl_provincias prv ON prv.`id` = p.`tbl_provincias_id_c` AND prv.`deleted` = 0
				JOIN pre_orden_pago opp ON opp.`pre_premios_id_c` = p.id AND COALESCE(opp.`pre_premios_cuotas_id_c`,'') = '' AND opp.`deleted` = 0
				JOIN accounts_cstm ac ON ac.id_c = opp.account_id_c  -- 23012022 lm - nuevo join toma permiso del accounts, agenet que pago 
				-- 23012022 lm se reemplaza por siguiente linea JOIN age_permiso prm ON prm.id = p.age_permiso_id_c AND prm.deleted = 0
				JOIN age_permiso prm ON prm.id_permiso = ac.id_permiso_c AND prm.deleted = 0
				JOIN age_personas per ON per.id = prm.`age_personas_id_c` AND per.deleted = 0
				LEFT JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'B' AND opb.deleted = 0
				/* desconectado 9/8/2018
				-- Sistema viejo hasta desconectar
				LEFT JOIN premios.`premios` pre ON pre.id_juego = prd.`id_as400` 
									AND pre.sorteo = ps.`nrosorteo` 
									AND pre.id_provincia = prv.`prv_id_boldt` 
									AND pre.nro_secuencia = p.`pre_secuencia`
									-- and pre.estado_premio = 1 -- no está confirmado
				*/
			WHERE p.`deleted` = 0
				AND p.`pre_pagaagencia` = 'S' -- paga agencia
				AND p.`pre_clasepremio` = 'B' -- Beneficiario
				AND p.`pre_estadopago` = 'A' -- pagado
				AND p.`pre_estinfuif` = 'N' -- solo retenciones!!!
				AND p.tbl_provincias_id_c = @var_santafe -- provincia santa fe
				AND opp.`opp_estado_registracion` <> 'D' -- provisorio
				AND DATE_FORMAT(opp.opp_fecha_pago, '%Y%m%d') BETWEEN '20171101' AND DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY), '%Y%m%d')
				-- ini corregido ae 10/1 -- si hay algo de info (cuit al menos) espero 5 días! y luego lo incorporo al tratamiento!
				AND (opb.id IS NULL 
					OR opb.opb_cuit IS NULL 
					OR DATE_FORMAT(COALESCE(opp.opp_fecha_pago, CURDATE()), '%Y%m%d')  <= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL @dias_gracia + @dias_gracia_adic DAY), '%Y%m%d') )
				AND per.`dni_cuit_titular_c` IS NOT NULL -- Agrego RL: se insertaban beneficiarios con name = null
			ORDER BY opp.`opp_fecha_pago`;
			
	ALTER TABLE tmp_pagos_a_informar
		ADD INDEX id (idPremio);
 
	SELECT COUNT(*) INTO @cnt
		FROM tmp_pagos_a_informar;
	IF @cnt = 0 THEN
		-- Indico la situacion
		SET @etapa = @etapa + 1;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - no hay OP a procesar - etapa ', @etapa, ' - fecha proceso ', CURDATE(), ' - hasta fecha ', DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY)));
		SET @msg ='OK';
		SELECT @msg INTO msgret;		
	ELSE
		-- Actualizo ORDEN DE PAGO
		SET @etapa = @etapa + 1;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - actualizo OP - etapa ', @etapa, ' - fecha proceso ', CURDATE(), ' - hasta fecha ', DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY)));
		-- Actualizo las ORDENES DE PAGO
			-- assigned_user_id (quien confirmó el proceso!)
			-- date_modified (cuando confirmó el proceso (ahora) !)
			-- llamo al PROCESO DE CONFIRMACION
			-- tipo de comprobante, talonario, letra, punto de venta, numero, fecha_comprobante
			-- estado_registración (D) (lo hace el proceso de confirmacion)
			-- estado_emision_ddjj (N) (lo hace el proceso de confirmacion)
			-- canal_de_pago (A)       (lo hace el proceso de confirmacion)
		UPDATE pre_orden_pago opp
			JOIN tmp_pagos_a_informar t ON t.idOPago = opp.id 
			     SET opp.description = CONCAT('Actualizado en forma autómatica durante el proceso del ', NOW()), 
			         opp.`assigned_user_id` = user_id,
				 opp.`date_entered` = @ahora;
		-- ini corregido ae 10/1
		
		-- Creo ELIMINO BENEFICIARIOS EXISTENTES
		SET @etapa = @etapa + 1;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - elimino beneficiarios - etapa ', @etapa, ' - fecha proceso ', CURDATE(), ' - hasta fecha ', DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY)));
		
		-- armar un temporal de beneficiarios para trabajar con 35 posiciones del ID
		-- la consulta que hacia join con left demora mucho tiempo
		
		DROP TEMPORARY TABLE IF EXISTS tmp_pre_orden_pago_beneficiarios;
		CREATE TEMPORARY TABLE tmp_pre_orden_pago_beneficiarios
			SELECT id, LEFT(id, 35) AS id_35 FROM pre_orden_pago_beneficiarios;
	
		ALTER TABLE tmp_pre_orden_pago_beneficiarios
		ADD INDEX id_35 (id_35), ADD INDEX id (id);	
		
		DELETE opbb  FROM tmp_pre_orden_pago_beneficiarios opb 
		INNER JOIN tmp_pagos_a_informar t   ON LEFT(t.idPremio,35) = id_35
		INNER JOIN pre_orden_pago_beneficiarios opbb ON opbb.id = opb.id;	
		
		-- DELETE opb FROM pre_orden_pago_beneficiarios opb
		--		JOIN tmp_pagos_a_informar t ON LEFT(t.idPremio,35) = LEFT(opb.id, 35);	
				
		-- ini corregido ae 10/1
		-- Creo BENEFICIARIOS
		SET @etapa = @etapa + 1;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - genero beneficiarios - etapa ', @etapa, ' - fecha proceso ', CURDATE(), ' - hasta fecha ', DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY)));
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
			per.`dni_cuit_titular_c`, -- Nombre
			@ahora, @ahora, 1, 1, NULL, 0, 1,
			p.id, -- orden de pago, igual que el premio
			'B',  -- tipo de beneficiario ("B")
			COALESCE(b.per_tipo, per.`tipo_persona`), -- tipo de persona
			per.`dni_cuit_titular_c`, 
			COALESCE(b.`per_nombre`, per.`name`), 
			COALESCE(b.`per_snombre`, ''),
			COALESCE(b.`per_apellido`, ''),
			COALESCE(b.`per_apemat`, per.`apellido_materno`), 
			COALESCE(b.`per_doc_tipo`, per.`titular_doc_tipo_c`),
			COALESCE(b.`per_doc_nro`, per.`titular_doc_nro_c`),
			COALESCE(b.`per_domi`, per.`billing_address_street`),
			COALESCE(b.`per_domi_calle`, ''), 
			COALESCE(b.`per_domi_nro`, ''), 
			COALESCE(b.`per_domi_piso`, ''), 
			COALESCE(b.`per_domi_dpto`, ''),
			COALESCE(b.`per_sexo`, per.`sexo_titular_c`), 
			COALESCE(b.`per_fechanac`, NULL), 
			COALESCE(b.`per_estado_civil`, '0'), -- SI NO VIENE, ES 0 = NO INFORMADO
			COALESCE(b.`per_email`, prm.`email_en_portal_c`), 
			COALESCE(b.`per_telefono_carac`, ''), 
			COALESCE(b.`per_telefono_numero`, ''),
			COALESCE(b.`per_pep`, 'N'), 
			COALESCE(b.`per_cargo`, ''),
			COALESCE(b.`tbl_provincias_id_c`, loc.`tbl_provincias_id_c`), -- provincia
			COALESCE(b.`tbl_localidades_id_c`, per.`tbl_localidades_id_c`), -- localidad
			COALESCE(b.`tbl_nacionalidades_id_c`, var_tbl_paises_id_c), -- nacionalidad SI NO VIENE, ES 1 ARGENTINA
			COALESCE(b.`tbl_ocupaciones_id_c`, var_tbl_ocupaciones_id_c), -- ocupacion SI NO VIENE, ES PERMISIONARIO
			COALESCE(b.`tbl_paises_id_c`, var_tbl_paises_id_c), -- pais de residencia
			COALESCE(b.`per_tipo_documental_afip`, '86'), -- tipo documental afip (por defecto CUIL 86)
			COALESCE(b.`per_tipo_soc`, ''), -- tipo de sociedad
			'', -- observaciones
			0,  
			'', 
			'', 
			'', 
			'',
			''
				FROM pre_premios p
					-- solo debo cargar estos premios!
					JOIN tmp_pagos_a_informar t ON t.idPremio = p.id
					JOIN sor_pgmsorteo ps ON ps.id = p.sor_pgmsorteo_id_c AND ps.deleted = 0
					JOIN pre_orden_pago opp ON opp.`pre_premios_id_c` = p.id AND COALESCE(opp.`pre_premios_cuotas_id_c`,'') = '' AND opp.`deleted` = 0
					JOIN accounts_cstm ac ON ac.id_c = opp.account_id_c  -- 23012022 lm - nuevo join toma permiso del accounts, agenet que pago 
					-- 23012022 lm se reemplaza por siguiente linea JOIN age_permiso prm ON prm.id = p.age_permiso_id_c AND prm.deleted = 0
					JOIN age_permiso prm ON prm.id_permiso = ac.id_permiso_c AND prm.deleted = 0
					JOIN age_personas per ON per.id = prm.`age_personas_id_c` AND per.deleted = 0
					LEFT JOIN tbl_localidades loc ON loc.id =  per.`tbl_localidades_id_c` AND loc.deleted = 0
					LEFT JOIN uif_persona b ON b.per_cuit = per.`dni_cuit_titular_c` AND b.`deleted` = 0
					LEFT JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'B' AND opb.deleted = 0
				WHERE per.`dni_cuit_titular_c` IS NOT NULL -- Agrego RL: se insertaban beneficiarios con name = null
				;
		-- llamo al proceso de confirmación para cada registro 
		
		SET @etapa = @etapa + 1;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - confirmo las cargas - etapa ', @etapa, ' - fecha proceso ', CURDATE(), ' - hasta fecha ', DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY)));
		CALL PREMIOS_genero_beneficiario_agencias_confirma();
		SET @etapa = @etapa + 1;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_genero_beneficiario_agencias - fin de proceso - etapa ', @etapa, ' - fecha proceso ', CURDATE(), ' - hasta fecha ', DATE_SUB(CURDATE(), INTERVAL @dias_gracia DAY)));
		SET @msg ='OK';
		-- select * from tmp_pagos_a_informar;
		-- drop TEMPORARY TABLE tmp_pagos_a_informar;
		SELECT @msg INTO msgret;
	END IF;
	
    END$$

DELIMITER ;