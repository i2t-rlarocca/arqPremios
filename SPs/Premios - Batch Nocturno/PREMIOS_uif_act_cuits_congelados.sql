DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_uif_act_cuits_congelados`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_uif_act_cuits_congelados`(OUT msgret VARCHAR(255))
fin:BEGIN
	-- DECLARE orden TINYINT;
	DECLARE salir INT DEFAULT 0;
	
		
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1
			@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
			@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' - PREMIOS_uif_act_cuits_congelados - Error ', COALESCE(@errno, 0), ' Mensaje ', COALESCE(@msg, '')));	
		SET msgret = 'Error';
	END;
	
	-- SET @orden = 2;
	SET @salir = 0, @etapa = 0;
	SELECT 'OK' INTO msgret;
	
	SET @etapa = @etapa + 1; -- 1
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - fecha proceso ', CURDATE()));
	
	/* 15-2-2024 - lmariotti - la tabla de cuits congelados, ya no sale del as400, sino desde administacion */
	
	TRUNCATE suitecrm_cas.pre_uif_rec_cuit_congelados;
	INSERT INTO suitecrm_cas.pre_uif_rec_cuit_congelados
	SELECT NAME AS cuit, 
	CONCAT(LEFT(fechaini,4), RIGHT(LEFT(fechaini,7),2), RIGHT(fechaini,2)) AS fec_ini,
	CONCAT(LEFT(fechafin,4), RIGHT(LEFT(fechafin,7),2), RIGHT(fechafin,2)) AS fec_fin
	FROM suitecrm_administracion_2019.`tg01_cuitsbloqueados`
	WHERE deleted = 0;
	/* fin 15-2-2024 - lmariotti */
		
		
	DROP TEMPORARY TABLE IF EXISTS pre_uif_aux_cuit_congelados;
	CREATE TEMPORARY TABLE pre_uif_aux_cuit_congelados (
	  id CHAR(36) NOT NULL,
	  cuit VARCHAR(11) DEFAULT NULL,
	  fec_ini VARCHAR(8) DEFAULT NULL,
	  fec_fin VARCHAR(8) DEFAULT NULL,
	  PRIMARY KEY (id)
	);
	
	SET @etapa = @etapa + 1; -- 2
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - inserto pre_uif_aux_cuit_congelados'));
	INSERT INTO pre_uif_aux_cuit_congelados
		SELECT ls.id, ls.cuit, DATE_FORMAT(ls.fecha_desde,'%Y%m%d') AS fec_ini, 
			CASE WHEN ls.fecha_hasta IS NOT NULL THEN DATE_FORMAT(ls.fecha_hasta,'%Y%m%d') ELSE ls.fecha_hasta END AS fec_fin
		FROM uif_persona_listas_seguimiento ls
		WHERE ls.uif_listas_seguimiento_id_c = '7e1f7b65-fb2f-d5df-0d03-53ea6686fca6' AND ls.deleted = 0
	;
	SET @etapa = @etapa + 1; -- 3
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - actualizo pre_uif_aux_cuit_congelados'));
	
	-- Actualizo. En AS el cuit es PK, si el mismo cuit esta congelado mas de una vez van pisando las fechas
	UPDATE pre_uif_aux_cuit_congelados a
	INNER JOIN pre_uif_rec_cuit_congelados r ON a.cuit = r.cuit
	SET a.fec_ini = r.fec_ini, a.fec_fin = r.fec_fin
	;
	
	SET @etapa = @etapa + 1; -- 4
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - creo kk_nuevos'));
	
	DROP TEMPORARY TABLE IF EXISTS kk_nuevos; -- tuve que recurrir a esto culpa del famoso error "cant reopen la tabla..."!!!
	CREATE TEMPORARY TABLE kk_nuevos (
		SELECT UUID() AS id, r.cuit, r.fec_ini, r.fec_fin
		FROM pre_uif_rec_cuit_congelados r 
		LEFT JOIN pre_uif_aux_cuit_congelados a ON r.cuit = a.cuit
		WHERE a.cuit IS NULL
	);
	
	SET @etapa = @etapa + 1; -- 5
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - inserto pre_uif_aux_cuit_congelados desde kk_nuevos'));
	INSERT INTO pre_uif_aux_cuit_congelados (id,cuit,fec_ini,fec_fin)
	SELECT * FROM kk_nuevos;
	-- ----------------------------------------------------------------------------
	IF NOT EXISTS(SELECT * FROM pre_uif_aux_cuit_congelados) THEN
		SET @salir = 1;
		SELECT 'No Existen novedades para procesar.' AS Mensaje INTO msgret;
		SELECT 'No Existen novedades para procesar.', cuit, 0 AS cant;
		LEAVE fin;
	END IF;
	
	-- controles "antes"
	
	SET @etapa = @etapa + 1; -- 6
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - creo tmp_dup'));
	DROP TEMPORARY TABLE IF EXISTS tmp_dup;
	CREATE TEMPORARY TABLE tmp_dup (
		SELECT 'CUIT-fecha_desde-fecha_hasta duplicado en tabla uif_persona_listas_seguimiento (a)' AS hay_duplicados, cuit, COUNT(1) AS cant, 0 AS deleted, CURDATE() AS fecha_desde, CURDATE() AS fecha_hasta FROM pre_uif_rec_cuit_congelados WHERE 1=0 GROUP BY cuit
	);
	SET @etapa = @etapa + 1; -- 7
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - inserto tmp_dup 1'));
	INSERT INTO tmp_dup
		SELECT 'CUIT duplicado en tabla pre_uif_rec_cuit_congelados (a)', cuit, COUNT(1), 0 AS deleted, NULL AS fecha_desde, NULL AS fecha_hasta FROM pre_uif_rec_cuit_congelados GROUP BY cuit HAVING COUNT(1) > 1
	;
	SET @etapa = @etapa + 1; -- 8
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - inserto tmp_dup 2'));
	INSERT INTO tmp_dup
		SELECT 'CUIT duplicado en tabla pre_uif_aux_cuit_congelados (a)', cuit, COUNT(1), 0 AS deleted, NULL AS fecha_desde, NULL AS fecha_hasta FROM pre_uif_aux_cuit_congelados GROUP BY cuit HAVING COUNT(1) > 1
	;
	SET @etapa = @etapa + 1; -- 9
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - inserto tmp_dup 3'));
	INSERT INTO tmp_dup
		SELECT 'CUIT-deleted duplicado en tabla uif_persona_listas_seguimiento (a)', cuit, COUNT(1),deleted, NULL AS fecha_desde, NULL AS fecha_hasta FROM uif_persona_listas_seguimiento  WHERE deleted = 0 GROUP BY cuit,deleted HAVING COUNT(1) > 1
	;
	SET @etapa = @etapa + 1; -- 10
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - inserto tmp_dup 4'));
	INSERT INTO tmp_dup
	SELECT 'CUIT-fecha_desde-fecha_hasta duplicado en tabla uif_persona_listas_seguimiento (a)', cuit, COUNT(1), 0 AS deleted,fecha_desde, fecha_hasta FROM uif_persona_listas_seguimiento WHERE deleted = 0 GROUP BY cuit,fecha_desde, fecha_hasta HAVING COUNT(1) > 1
	;
	IF EXISTS(SELECT * FROM tmp_dup) THEN
		SET @salir = 1;
		SELECT 'Existen cuits duplicados. Verifique con Sistemas' AS Mensaje INTO msgret;
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - inicio - etapa ', @etapa, ' - ERROR - EXISTEN CUITS DUPLICADOS'));
		SELECT * FROM tmp_dup;
		LEAVE fin;
	END IF;
	
	-- ----------------------------------------------------------------------------
	SET @etapa = @etapa + 1; -- 11
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - verificacion inicial - etapa ', @etapa, ' - estado ', @salir));
	
	IF (@salir = 0) THEN -- solo para claridad de flujo...
		-- insert into kk_auditoria select CONCAT(DATE_FORMAT(NOW(),'%d/%m%/%Y %H:%i:%S'),' - ETL CUITs Congelados - entro');
		
		-- hay una pedorra en AS!!!--------------------------------------------------------------------------------------------------
		UPDATE pre_uif_aux_cuit_congelados
				SET fec_fin = '20150301'
			WHERE fec_fin = '20150230';
		-- --------------------------------------------------------------------------------------------------------------------------
		UPDATE uif_persona_listas_seguimiento ls 
			JOIN pre_uif_aux_cuit_congelados t
				ON ls.cuit = t.cuit 
			SET ls.fecha_desde = CONCAT(LEFT(t.fec_ini,4), "-", RIGHT(LEFT(t.fec_ini,6),2), "-", RIGHT(t.fec_ini,2)) , 
			    ls.fecha_hasta = CONCAT(LEFT(t.fec_fin,4), "-", RIGHT(LEFT(t.fec_fin,6),2), "-", RIGHT(t.fec_fin,2)),
				ls.date_modified = NOW()
			WHERE ls.uif_listas_seguimiento_id_c = '7e1f7b65-fb2f-d5df-0d03-53ea6686fca6' AND ls.deleted = 0 
				AND (CONCAT(LEFT(t.fec_ini,4), "-", RIGHT(LEFT(t.fec_ini,6),2), "-", RIGHT(t.fec_ini,2)) <> ls.fecha_desde
				     OR CONCAT(LEFT(t.fec_fin,4), "-", RIGHT(LEFT(t.fec_fin,6),2), "-", RIGHT(t.fec_fin,2)) <> ls.fecha_hasta); 
		-- INSERT INTO kk_auditoria SELECT CONCAT(DATE_FORMAT(NOW(),'%d/%m%/%Y %H:%i:%S'),' - ETL CUITs Congelados - antes de insert');
		INSERT INTO uif_persona_listas_seguimiento (id, NAME, date_entered, date_modified, modified_user_id, created_by, description, deleted, assigned_user_id, cuit, uif_listas_seguimiento_id_c, fecha_desde, fecha_hasta, uif_persona_id_c)
		SELECT t.id AS id, 
			t.cuit AS NAME, 
			NOW() AS date_entered, 
			NOW() AS date_modified, 
			1 AS modified_user_id, 
			1 AS created_by, 
			CONCAT("CUIT: ", t.cuit, ".") AS description, 
			0 AS deleted, 
			1 AS assigned_user_id, 
			t.cuit AS cuit, 
			'7e1f7b65-fb2f-d5df-0d03-53ea6686fca6' AS uif_listas_seguimiento_id_c, 
			CONCAT(LEFT(t.fec_ini,4), "-", RIGHT(LEFT(t.fec_ini,6),2), "-", RIGHT(t.fec_ini,2)) AS fecha_desde,
			CONCAT(LEFT(t.fec_fin,4), "-", RIGHT(LEFT(t.fec_fin,6),2), "-", RIGHT(t.fec_fin,2)) AS fecha_hasta,
			"" AS uif_persona_id_c
			FROM pre_uif_aux_cuit_congelados t
				LEFT JOIN uif_persona_listas_seguimiento ls
					ON ls.cuit = t.cuit AND ls.uif_listas_seguimiento_id_c = '7e1f7b65-fb2f-d5df-0d03-53ea6686fca6'
			WHERE ls.cuit IS NULL ORDER BY NAME;
		-- INSERT INTO kk_auditoria SELECT CONCAT(DATE_FORMAT(NOW(),'%d/%m%/%Y %H:%i:%S'),' - ETL CUITs Congelados - antes de update');
		UPDATE uif_persona_listas_seguimiento ls
			LEFT JOIN pre_uif_aux_cuit_congelados t
				ON ls.cuit = t.cuit 
			SET ls.deleted = 1, 
				ls.date_modified = NOW()
			WHERE ls.deleted = 0
			AND t.cuit IS NULL -- no vinieron en el vuelco
			AND ls.uif_listas_seguimiento_id_c = '7e1f7b65-fb2f-d5df-0d03-53ea6686fca6' -- Inhibidos por AFIP
		; 
			
		-- controles "despues"
		INSERT INTO kk_auditoria SELECT CONCAT(DATE_FORMAT(NOW(),'%d/%m%/%Y %H:%i:%S'),' - ETL CUITs Congelados - antes de control despues');
		DROP TEMPORARY TABLE IF EXISTS tmp_dup;
		CREATE TEMPORARY TABLE tmp_dup (
			SELECT 'CUIT-fecha_desde-fecha_hasta duplicado en tabla uif_persona_listas_seguimiento (d)' AS hay_duplicados, cuit, COUNT(1) AS cant, 0 AS deleted, CURDATE() AS fecha_desde, CURDATE() AS fecha_hasta FROM pre_uif_rec_cuit_congelados WHERE 1=0 GROUP BY cuit
		);
		INSERT INTO tmp_dup
			SELECT 'CUIT duplicado en tabla pre_uif_rec_cuit_congelados (d)', cuit, COUNT(1), 0 AS deleted, NULL AS fecha_desde, NULL AS fecha_hasta FROM pre_uif_rec_cuit_congelados GROUP BY cuit HAVING COUNT(1) > 1
		;
		INSERT INTO tmp_dup
			SELECT 'CUIT duplicado en tabla pre_uif_aux_cuit_congelados (d)', cuit, COUNT(1), 0 AS deleted, NULL AS fecha_desde, NULL AS fecha_hasta FROM pre_uif_aux_cuit_congelados GROUP BY cuit HAVING COUNT(1) > 1
		;
		INSERT INTO tmp_dup
			SELECT 'CUIT-deleted duplicado en tabla uif_persona_listas_seguimiento (d)', cuit, COUNT(1),deleted, NULL AS fecha_desde, NULL AS fecha_hasta FROM uif_persona_listas_seguimiento  WHERE deleted = 0 GROUP BY cuit,deleted HAVING COUNT(1) > 1
		;
		INSERT INTO tmp_dup
			SELECT 'CUIT-fecha_desde-fecha_hasta duplicado en tabla uif_persona_listas_seguimiento (d)', cuit, COUNT(1), 0 AS deleted, fecha_desde, fecha_hasta FROM uif_persona_listas_seguimiento WHERE deleted = 0 GROUP BY cuit,fecha_desde, fecha_hasta HAVING COUNT(1) > 1
		;
		IF EXISTS(SELECT * FROM tmp_dup) THEN
			SET @salir = 1;
			SELECT 'Existen cuits duplicados. Verifique con Sistemas' AS Mensaje INTO msgret;
			SELECT * FROM tmp_dup;
			LEAVE fin;
		END IF;	
		
		-- ----------------------------------------------------------------------------		
		-- TAREAS DE INICIO DE DIA

		
		SET @etapa = @etapa + 1; -- 14
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - genero beneficiarios - etapa ', @etapa));
		CALL PREMIOS_genero_beneficiario_agencias('1', @msg);
		IF (COALESCE(@msg, '') <> 'OK' AND COALESCE(@msg, '') <> 'Los domingos no procesa...') THEN
			SET @salir = 1;
			SELECT 'Problema al ejecutar el SP que genera beneficiarios agencias. Verifique con Sistemas' AS Mensaje INTO msgret;
			INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - ERROR en generacion de beneficiarios - etapa ', @etapa, ' - mensaje ', @msg));
			LEAVE fin;
		END IF;	
		
		
		SET @etapa = @etapa + 1; -- 17
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Actualiza premios QNL repetidos - etapa ', @etapa));
		CALL sor_actualiza_premios_qnl_repetidos(@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		IF (@RCode <> 0) THEN
			SET @salir = 1;
			SELECT 'Problema al ejecutar el SP que actualiza premios QNL repetidos. Verifique con Sistemas' AS Mensaje INTO msgret;
			INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - ERROR al ejecutar el SP que actualiza premios QNL repetidos - etapa ', @etapa, ' - mensaje ', @msg));
			LEAVE fin;
		END IF;	
		
		
	
		-- actualiza personas UIF y ejecuta la generacion de operaciones UIF
		
		SET @etapa = @etapa + 1; -- 19
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - actualizacion personas/operaciones UIF - etapa ', @etapa));
		
		CALL PREMIOS_uif_act_personas(msgret);
		IF (msgret <> 'OK') THEN
			SET @salir = 1;
			INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - ERROR al ejecutar el SP PREMIOS_uif_act_personas ', msgret));
			SELECT 'Error en PREMIOS_uif_act_personas' INTO msgret;			
			LEAVE fin;
		END IF;		
		
		
		-- marca cerradas los sorteo de los juegos que no son q6-br-lot, porque los cierran los usuarios
		
		SET @etapa = @etapa + 1; -- 20
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Cierra emisiones prescriptas (no q6-br-lot) - etapa ', @etapa));
		
		UPDATE  sor_pgmsorteo a
			INNER JOIN sor_producto b ON b.id = a.sor_producto_id_c
		SET sor_emision_cerrada = 1, sor_fechor_emision_cerrada = NOW(), sor_presc_contabilizada = 1, sor_fechor_presc_contabilizada = NOW()
		WHERE  a.deleted = 0 AND b.id_juego NOT IN (50, 13, 4, 5, 29) -- lot, br, q6, loto y loto 5
			AND sor_emision_cerrada = 0
			AND sor_presc_recibida = 1
			AND a.fecha < DATE_SUB(CURDATE(), INTERVAL 90 DAY);
		
		SET @etapa = @etapa + 1; -- 21
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura pre_premios_menores_historicos - etapa ', @etapa));
		

		SET @etapa = @etapa + 1; -- 22
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Incorpora pre_premios_menores_historicos - etapa ', @etapa));

		CREATE TEMPORARY TABLE tmp_borrar_menores
			SELECT DISTINCT p.id 
			FROM pre_premios_menores p
				JOIN sor_pgmsorteo ps FORCE INDEX (fecha) ON ps.id = p.sor_pgmsorteo_id_c AND ps.deleted = 0
				LEFT JOIN pre_premios_menores_historicos h ON h.id = p.id
				WHERE ps.sor_emision_cerrada = 1 -- Cerrados
				AND ps.fecha < DATE_SUB(CURDATE(), INTERVAL 90 DAY)
				-- AND COALESCE(ps.sor_fechor_emision_cerrada,'000-00-00 00:00:00') < CONCAT(DATE_SUB(CURDATE(), INTERVAL 45 DAY), ' 23:59:59')
				AND ps.sor_presc_contabilizada = 1
				AND h.id IS NULL
		;
		INSERT INTO pre_premios_menores_historicos
			SELECT p.* FROM pre_premios_menores p
				JOIN sor_pgmsorteo ps FORCE INDEX (fecha) ON ps.id = p.sor_pgmsorteo_id_c AND ps.deleted = 0
				LEFT JOIN pre_premios_menores_historicos h ON h.id = p.id
				WHERE ps.sor_emision_cerrada = 1 -- Cerrados
				AND ps.fecha < DATE_SUB(CURDATE(), INTERVAL 90 DAY)
				-- AND COALESCE(ps.sor_fechor_emision_cerrada,'000-00-00 00:00:00') < CONCAT(DATE_SUB(CURDATE(), INTERVAL 45 DAY), ' 23:59:59')
				AND ps.sor_presc_contabilizada = 1
				AND h.id IS NULL;
						
		SET @etapa = @etapa + 1; -- 23
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura pre_premios_menores - etapa ', @etapa));
		
		DELETE p.* FROM pre_premios_menores p
		INNER JOIN tmp_borrar_menores t ON p.id = t.id
		;

		DROP TEMPORARY TABLE IF EXISTS tmp_borrar_menores;
		
		-- depuracion de los prescritos de cuenta corriente
		SET @etapa = @etapa + 1; -- 24
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura cas02_cc_prescripcion_cab - etapa ', @etapa));

		DELETE a.* FROM `cas02_cc_prescripcion_cab` a
		INNER JOIN sor_pgmsorteo ps ON ps.sor_producto_id_c = a.cas02_cc_juegos_id_c
		INNER JOIN sor_producto pr FORCE INDEX (PRIMARY)  ON pr.id = ps.sor_producto_id_c AND ps.nrosorteo = a.ac_sorteo
		WHERE ps.sor_emision_cerrada = 1
		AND ps.fecha < DATE_SUB(CURDATE(), INTERVAL 270 DAY);

		SET @etapa = @etapa + 1; -- 25
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura cas02_cc_prescripcion_det - etapa ', @etapa));
		
		DELETE a.* FROM  `cas02_cc_prescripcion_det` a
		INNER JOIN `cas02_cc_prescripcion_det_c` b ON b.`cas02_cc_a3529ion_det_idb` = a.id
		LEFT JOIN `cas02_cc_prescripcion_cab` c ON c.id = b.`cas02_cc_ab25bion_cab_ida` 
		WHERE c.id IS NULL;
		
		SET @etapa = @etapa + 1; -- 26
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura cas02_cc_prescripcion_det_c - etapa ', @etapa));

		DELETE  a.* FROM `cas02_cc_prescripcion_det_c` a
		LEFT JOIN `cas02_cc_prescripcion_cab` b ON b.id = a.`cas02_cc_ab25bion_cab_ida`
		WHERE b.deleted IS NULL;			
					
		SET @etapa = @etapa + 1; -- 27
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura ventas inusuales - etapa ', @etapa));
		DELETE p FROM `gar_vta_inusuales` p
			WHERE p.fecha < DATE_SUB(CURDATE(), INTERVAL 90 DAY);
		
                         
                SET @etapa = @etapa + 1; -- 28
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura kk_auditoria - etapa ', @etapa));
		DELETE p FROM kk_auditoria p
                         WHERE  SUBSTR(p.nombre, 1, 10)   <  DATE_SUB(CURDATE(), INTERVAL 15 DAY);
                         
                DELETE p FROM kk_auditoria p WHERE p.nombre IS NULL;
                         
                SET @etapa = @etapa + 1; -- 29
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura kk - etapa ', @etapa));
		DELETE p FROM kk p
                         WHERE  SUBSTR(p.nombre, 1, 10)   <  DATE_SUB(CURDATE(), INTERVAL 15 DAY);
                        
                SET @etapa = @etapa + 1; -- 30
		INSERT INTO kk_auditoria (nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Depura kk_auditoria2 - etapa ', @etapa));
		DELETE p FROM kk_auditoria2 p
                         WHERE  SUBSTR(p.nombre, 1, 10)   <  DATE_SUB(CURDATE(), INTERVAL 15 DAY);
                         
                DELETE p FROM kk_auditoria2 p WHERE p.nombre IS NULL;
                         

                              
                SET @etapa = @etapa + 1; -- 33
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - Actualiza premios enviados as400 - etapa ', @etapa));
		UPDATE pre_orden_pago SET opp_estado_registracion = 'D'
			WHERE  opp_estado_registracion = 'P' AND estado_envio_as400 = 'E';	
                          
		
		SET @etapa = @etapa + 1; -- 34
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - delete sor_aud_consolidacion_dif - etapa ', @etapa));
			
		DELETE p 
		FROM `sor_aud_consolidacion_dif`  p
		INNER JOIN sor_pgmsorteo a ON p.idpgmsorteo = a.idpgmsorteo
		WHERE  a.fecha < DATE_SUB(CURDATE(), INTERVAL 60 DAY);
		

		
						
		SET @etapa = @etapa + 1; -- 40
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - actualiza fecha premios tesoreria - etapa ', @etapa));			
		
		UPDATE suitecrm_cas.pre_orden_pago
		SET opp_fecha_pago = CONCAT(opp_fecha_comprobante, ' 10:00:00'), opp_estado_actualizacion_fpag = 'C'
		WHERE opp_estado_actualizacion_fpag IN ('C', 'D') -- valor correcto es C
		AND opp_estado_registracion = 'D'
		AND COALESCE(opp_fecha_pago,'') = '';
								
		SET @etapa = @etapa + 1; -- 41
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - actualiza estado registracion premios - etapa ', @etapa));			
		
		UPDATE  suitecrm_cas.pre_premios p
		JOIN suitecrm_cas.pre_orden_pago opp ON opp.pre_premios_id_c = p.id
		SET opp.opp_estado_registracion = 'D'  
		WHERE p.pre_estregistrobenef = 'C' AND opp.name LIKE 'W%' AND opp.opp_estado_registracion <> 'D';
		SET @etapa = @etapa + 1; -- 48
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_uif_act_cuits_congelados - finalizo tareas de la mañana - etapa ', @etapa));
		
	END IF;
	
END$$

DELIMITER ;