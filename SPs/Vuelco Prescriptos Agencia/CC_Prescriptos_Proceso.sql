DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `CC_Prescriptos_Proceso`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `CC_Prescriptos_Proceso`(IN id_proceso INT,IN id_secuencia INT,IN usuario VARCHAR(255), IN tipo_validacion CHAR(3),OUT msgret VARCHAR(2048))
fin:BEGIN
	DECLARE c_juego INT;
	DECLARE c_sorteo INT; 
	DECLARE c_idpgmsorteo CHAR(36);
	DECLARE msgerr VARCHAR(2048);
	-- declare msgret VARCHAR(2048);
	DECLARE done INT DEFAULT 0;
	DECLARE mi_cursor3 CURSOR FOR 
		 SELECT 	p.nro_juego AS juego, 
				p.nro_sorteo AS sorteo,
				s.id AS id_pgmsorteo
		 FROM sor_rec_prescrip p
			 INNER JOIN sor_pgmsorteo s ON s.idjuego = p.nro_juego AND s.nrosorteo = p.nro_sorteo
			-- INNER JOIN pre_premios pr ON pr.sor_pgmsorteo_id_c = s.id -- QUITAR PARA PASAR A TESTING ESTA SOLO PARA DATOS DE PRUEBA UNITARIA
		 WHERE p.id_proceso = @id_proceso -- and p.codigo_concepto IN (15,17,4)  
		 GROUP BY p.nro_juego, p.nro_sorteo,s.id;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	SET @cod_err = '0';	
	SET @usuario = usuario;  
	SET @id_proceso = id_proceso;
	SET @liquidacion = id_secuencia;
	INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - inicio - proceso', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	
			
	/*	VAIDACION SECUENCIA	*/	
	/*	VALIDA SI LOS ARCHIVOS TRAEN DATOS	*/
	
	IF (tipo_validacion = 'PPR') THEN
		
		-- actualiza numero de liquidacion
		UPDATE sor_rec_prescrip  
			SET 	nro_liquidacion = CAST(@liquidacion AS UNSIGNED),
			-- fecha_sorteo=v_fecha_liquidacion, ;
				id_detalle = UUID(),
				sec_liquidacion = 0
		;
			
		-- id de cabecera....
		UPDATE sor_rec_prescrip a
			INNER JOIN (
				SELECT  	UUID() AS id_cab, 
						t.nro_juego, 
						t.nro_sorteo, 
						t.fecha_sorteo, 
						t.agente,
						t.subagente,
						t.vendedor,
						t.cuit ,
						t.agente_originante,
						t.subagente_originante,
						t.vendedor_originante,
						t.cuit_calculado,
						t.permiso,
						t.id_cuenta,
						t.nro_liquidacion,
						t.sec_liquidacion
				FROM sor_rec_prescrip t 
				WHERE t.nro_liquidacion = CAST(@liquidacion AS UNSIGNED)
				GROUP BY t.nro_juego, t.nro_sorteo, t.fecha_sorteo, t.agente,t.subagente,	t.vendedor,t.cuit ,t.agente_originante,t.subagente_originante,t.vendedor_originante ,t.cuit_calculado,
					t.permiso,t.id_cuenta,t.nro_liquidacion,	t.sec_liquidacion
					) i ON i.nro_juego = a.nro_juego AND i.nro_sorteo = a.nro_sorteo AND i.fecha_sorteo = a.fecha_sorteo 
						AND i.agente = a.agente AND i.subagente = a.subagente AND i.vendedor = i.vendedor AND i.cuit = i.cuit
						AND i.agente_originante = a.agente_originante AND i.subagente_originante = a.subagente_originante
						AND i.vendedor_originante = a.vendedor_originante AND i.cuit_calculado = a.cuit_calculado
						AND i.permiso = a.permiso AND i.id_cuenta = a.id_cuenta AND i.nro_liquidacion = a.nro_liquidacion
						AND i.sec_liquidacion = a.sec_liquidacion 
			SET a.id_padre = i.id_cab;
			INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - inicio - elimino liquidacion', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		
			DELETE  a FROM cas02_cc_prescripcion_det_c  a
			INNER JOIN cas02_cc_prescripcion_cab cab ON a.cas02_cc_ab25bion_cab_ida=cab.id AND cab.deleted=0
			-- INNER JOIN cas02_cc_prescripcion_det det ON a.cas02_cc_a3529ion_det_idb=det.id AND det.deleted=0
			WHERE cab.ac_liquidacion= CAST(@liquidacion AS UNSIGNED); -- AND det.ad_liquidacion=CAST(@liquidacion AS UNSIGNED);
			
			DELETE a2 FROM  cas02_cc_precab_accounts_c a2
			INNER JOIN cas02_cc_prescripcion_cab cab ON a2.cas02_cc_a9e73ion_cab_idb=cab.id
			WHERE cab.ac_liquidacion=CAST(@liquidacion AS UNSIGNED);
			
			DELETE FROM cas02_cc_prescripcion_det  WHERE ((ad_liquidacion=CAST(@liquidacion AS UNSIGNED)));	
			DELETE FROM cas02_cc_prescripcion_cab  WHERE ((ac_liquidacion=CAST(@liquidacion AS UNSIGNED)));
			INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - inicio - inserto liquidacion cab ', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
			
			-- inserta cabecera
			INSERT INTO cas02_cc_prescripcion_cab(id, NAME,date_entered,date_modified,modified_user_id,created_by,
			description,deleted,assigned_user_id,ac_sorteo,ac_fsorteo,ac_apuestas,
			ac_total_neto,currency_id,cas02_cc_juegos_id_c,account_id_c,ac_cupones,ac_secuencia,
			ac_liquidacion,ac_estado,ac_cuit)
		
			-- reemplazo MM
			SELECT  t.id_padre, CONCAT(c.tipo_archivo,'_',c.secuencia_liquidacion,'_',c.nro_liquidacion), NOW(),NOW(), @usuario, @usuario,
			   '',0, '1', t.nro_sorteo, t.fecha_sorteo, 0,
			 SUM(CASE WHEN t.codigo_concepto = 99 THEN t.importe ELSE 0 END) AS importe 
			 , '1', ps.sor_producto_id_c, t.id_cuenta, 0, c.secuencia_liquidacion,
			 c.nro_liquidacion, c.tipo_liquidacion, t.cuit
			FROM sor_rec_prescrip_ctrl c
				INNER JOIN sor_rec_prescrip t ON c.nro_liquidacion = t.nro_liquidacion
				INNER JOIN sor_pgmsorteo ps ON ps.idjuego = t.nro_juego AND ps.nrosorteo = t.nro_sorteo AND ps.deleted = 0
			WHERE c.nro_liquidacion = CAST(@liquidacion AS UNSIGNED)	
			GROUP BY t.nro_juego, t.nro_sorteo, t.fecha_sorteo, t.agente,t.subagente,
				t.vendedor,t.cuit ,t.agente_originante,t.subagente_originante,t.vendedor_originante ,t.cuit_calculado,
				t.permiso,t.id_cuenta,t.nro_liquidacion,t.sec_liquidacion;
			
			SET @cab_cant = -1;
		
			SELECT COUNT(*) INTO @cab_cant
				FROM cas02_cc_prescripcion_cab c
				WHERE c.ac_liquidacion = CAST(@liquidacion AS UNSIGNED);  		
				
			IF (@cab_cant <= 0) THEN
						SET @cod_err = '14';
						SET @msgaud=CONCAT("Codigo error: ",@cod_err ," Problema en vuelco en tabla cas02_cc_prescripcion_cab");
						CALL sor_inserta_auditoria(@id_proceso,99, @id_secuencia,203,@usuario,202,204, @msgaud);
						SET msgret="Problema en vuelco en tabla cas02_cc_prescripcion_cab - no hay registros para procesar";
						LEAVE fin;-- sale del SP
			END IF;
			
			INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - inicio - inserto liquidacion det ', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
			
			INSERT INTO cas02_cc_prescripcion_det(id,NAME,date_entered,date_modified,modified_user_id,created_by,description,
			deleted,assigned_user_id,ad_secuencia,ad_constancia,ad_importe,currency_id,ad_liquidacion,ad_alicuota,cas02_cc_conceptos_id_c,
			agente_originante,subagente_originante,vendedor_originante,permiso_originante)
			
			SELECT  r.id_detalle, con.name, NOW(),NOW(),NULL,NULL,con.name,
			0,NULL,r.sec_liquidacion, r.nro_constancia,
			r.importe, r.moneda, r.nro_liquidacion,r.alicuota,r.codigo_concepto,
			r.agente_originante,r.subagente_originante,r.vendedor_originante,r.permiso_originante
			FROM sor_rec_prescrip r
			INNER JOIN cas02_cc_conceptos con ON con.co_id = r.codigo_concepto AND con.deleted=0
			WHERE r.nro_liquidacion = CAST(@liquidacion AS UNSIGNED);
			
			SET @det_cant = -1;
			SELECT COUNT(*) INTO @det_cant
				FROM cas02_cc_prescripcion_det  d
				WHERE d.ad_liquidacion = CAST(@liquidacion AS UNSIGNED); 
			
			
			IF (@det_cant  <= 0) THEN
						SET @cod_err = '15';
						SET @msgaud=CONCAT("Codigo error: ",@cod_err ," Problema en vuelco en tabla cas02_cc_prescripcion_det");
						CALL sor_inserta_auditoria(@id_proceso,99, @id_secuencia,203,@usuario,202,204, @msgaud);
						SET msgret="Problema en vuelco en tabla cas02_cc_prescripcion_det";
						LEAVE fin;-- sale del SP
			END IF;
			INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - inicio - inserto liquidacion rel ', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		
			-- inserto en  tabla de relacion  cas02_cc_prescripcion_det_c
			INSERT INTO cas02_cc_prescripcion_det_c(id,date_modified,deleted,cas02_cc_ab25bion_cab_ida,cas02_cc_a3529ion_det_idb)
			SELECT UUID(), NOW(),0,rex.id_padre, rex.id_detalle
			FROM  sor_rec_prescrip rex
			WHERE rex.nro_liquidacion = CAST(@liquidacion AS UNSIGNED);
			
			SET @det_c = -1;
			SELECT COUNT(*) INTO @det_c
			FROM cas02_cc_prescripcion_det_c a
				INNER JOIN cas02_cc_prescripcion_cab cab ON a.cas02_cc_ab25bion_cab_ida=cab.id AND cab.deleted=0
					WHERE cab.ac_liquidacion = CAST(@liquidacion AS UNSIGNED);  				
			
			IF (@det_c  <= 0) THEN
						SET @cod_err = '16';
						SET @msgaud=CONCAT("Codigo error: ",@cod_err ," Problema en vuelco en tabla cas02_cc_prescripcion_det_c");
						CALL sor_inserta_auditoria(@id_proceso,99, @id_secuencia,203,@usuario,202,204, @msgaud);
						SET msgret="Problema en vuelco en tabla cas02_cc_prescripcion_det_c";
						LEAVE fin;-- sale del SP
			END IF;
		SET @msgaud="Finalización correcta de proceso Prescriptos a tablas definitivas";
		CALL sor_inserta_auditoria(@id_proceso,99, @id_secuencia,203,@usuario,202,204, @msgaud);
		SET msgret='OK';
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), 'cc_prescriptos_proceso - fin - proceso', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		LEAVE fin;-- sale del SP
	END IF;
	IF (tipo_validacion = 'RES') THEN
		OPEN mi_cursor3;
		read_loop: LOOP
		FETCH mi_cursor3 INTO c_juego, c_sorteo, c_idpgmsorteo;
			-- llamar a cierre_presc_emision RESUMEN DE EMISION
			SET @msgerr = '';
			-- el parametro id_ordenpago (segundo) se utilizará para pasar la tabla de origen del ticket para el cierre 
			
			INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - ini cierre ', c_idpgmsorteo, ' juego ', c_juego, ' sorteo ', c_sorteo, ' resultado ', @msgerr));
			-- CALL PREMIOS_CIERRE_presc_emision(c_idpgmsorteo,'',0,3,@msgerr);
			CALL PREMIOS_CIERRE_presc_emision(c_idpgmsorteo,'',@liquidacion,3,@msgerr);
			INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - fin cierre ', c_idpgmsorteo, ' juego ', c_juego, ' sorteo ', c_sorteo, ' resultado ', @msgerr));
			
			IF (@msgerr != 'OK') THEN
				
				SET @cod_err = '17';
				SET @msgaud=CONCAT("Codigo error: ",@cod_err ," Generando resumen de emision de prescriptos canal de pago agencia : ",msgerr);
				CALL sor_inserta_auditoria(@id_proceso,99, @id_secuencia,203,@usuario,202,204, @msgaud);
				SET msgret = CONCAT("Problema generando resumen de emision de prescriptos canal de pago agencia");
				LEAVE read_loop;
				LEAVE fin;
				
			END IF;
    
			IF done = 1 THEN
				LEAVE read_loop;
			END IF; 
			
		END LOOP read_loop;
		CLOSE mi_cursor3;
		UPDATE sor_rec_prescrip_ctrl 
				SET presc_procesada = 1
			WHERE id_proceso = @id_proceso;
		SET @msgaud="Finalización correcta de proceso Prescriptos a tablas definitivas";
		CALL sor_inserta_auditoria(@id_proceso,99, @id_secuencia,203,@usuario,202,204, @msgaud);
		SET msgret='OK';
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_proceso - fin - proceso', id_proceso, ' id_secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		LEAVE fin;-- sale del SP
	END IF;
    END$$

DELIMITER ;