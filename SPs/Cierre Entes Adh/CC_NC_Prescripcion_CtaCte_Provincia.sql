DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `CC_NC_Prescripcion_CtaCte_Provincia`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `CC_NC_Prescripcion_CtaCte_Provincia`(
	IN param_lst_str_ids   VARCHAR(4096),
	IN par_idUsuario       VARCHAR(36),
	OUT RCode	       INT,
	OUT RTxt	       VARCHAR(500),
	OUT RId		       VARCHAR(36),
	OUT RSQLErrNo	       INT,
	OUT RSQLErrtxt	       VARCHAR(500)
)
thisSP:
BEGIN /*
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	    BEGIN
	      GET DIAGNOSTICS CONDITION 1
		@code = RETURNED_SQLSTATE,
		@msg = MESSAGE_TEXT,
		@errno = MYSQL_ERRNO,
		@base = SCHEMA_NAME,
		@tabla = TABLE_NAME;
	      SET RCode = 0;
	      SET RTxt = "Excepción SQL";
	      SET RId = 0;
	      SET RSQLErrNo = @errno;
	      SET RSQLErrtxt = CONCAT('CC_NC_Prescripcion_CtaCte_Provincia ', @msg);
	      
	      INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - sqlException: RSQLErrNo ->', RSQLErrNo,'<- RSQLErrtxt ->',RSQLErrtxt,'<-'));
	    END;
	*/	
	SET RCode = 1 ;
	SET RTxt = 'OK' ;
	SET RSQLErrNo = 0 ;
	SET RSQLErrtxt = "OK" ;
	SET RId = 0 ;
	-- START TRANSACTION;
	SET @param_lst_str_ids = param_lst_str_ids;
	-- CALL sor_inserta_auditoria(@id_proceso,juego, sorteo,84,usuario,81,85,"NCs Pcias - INICIO ");
	SET @lstNroSorteos = '';
	SET @prod_name = '';
	SET @prod_id = -1;
	SET @query3 = "";
	SET @query3 = CONCAT(@query3,
		"SELECT prod_id, prod_name, GROUP_CONCAT(nrosorteo,'-')
		INTO @prod_id, @prod_name, @lstNroSorteos
		FROM (
			SELECT         
			    DISTINCT p.id_juego as prod_id, trim(p.name) as prod_name, ps.id, ps.idjuego, ps.nrosorteo
			FROM pre_resumen_emision re 
			INNER JOIN sor_pgmsorteo ps ON ps.id = re.sor_pgmsorteo_id_c
			INNER JOIN sor_producto p on p.id = ps.sor_producto_id_c
			INNER JOIN tbl_listas_desplegables ld ON ld.codigo = re.estado
			WHERE re.sor_pgmsorteo_id_c IN (", @param_lst_str_ids ,")
			AND (ps.sor_emision_cerrada = 1 AND re.estado = '2') 
			order by ps.idjuego, ps.nrosorteo desc
		    ) a 
		GROUP BY prod_id,prod_name
		order by nrosorteo
		         ;")
	;
	PREPARE sentenciasql3 FROM @query3;
	-- SELECT @query3;
	EXECUTE sentenciasql3
	;
	SET @lstNroSorteos = REPLACE(CASE WHEN RIGHT(TRIM(@lstNroSorteos),1) = '-' THEN LEFT(@lstNroSorteos, LENGTH(@lstNroSorteos) -1) ELSE @lstNroSorteos END,',',''); 
	
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - Inicio - Sorteos: ', @lstNroSorteos, ' - IDs: ', @param_lst_str_ids));
	
	-- Creo tabla temporal tmp_emisiones
	DROP TEMPORARY TABLE IF EXISTS tmp_emisiones; 
	CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emisiones (	
		id_red	    		CHAR(36),
		razon_social     	VARCHAR(255),
		provincia 		CHAR(36),
		retencion           	DECIMAL(18,2),
		id_sorteo               CHAR(36),
		sorteo                  VARCHAR(255),
		producto                VARCHAR(255),
		lstNroSorteos           VARCHAR(4096),
		total_liquidado           	DECIMAL(18,2)
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
	SET @query = "";
	SET @query = CONCAT(@query,
			"INSERT INTO tmp_emisiones (id_red,razon_social,provincia,retencion,id_sorteo,sorteo,producto,lstNroSorteos,total_liquidado) 
			SELECT 
				a.age_red_id_c, 
				a.razon_social, 
				c.tbl_provincias_id_c, 
				presc.retenciones_gan, 
				presc.id AS id_sorteo, 
				presc.nombre_sorteo as sorteo,
				@prod_name AS producto,
				@lstNroSorteos,
				tot_liq.retenciones_gan AS total_liquidado
			FROM age_permiso a
			INNER JOIN age_local b ON a.age_local_id_c = b.id
			INNER JOIN tbl_localidades c ON c.id = b.tbl_localidades_id_c
			INNER JOIN (
				SELECT  ps.id, rp.tbl_provincias_id_c,  
					ps.name AS nombre_sorteo,
					SUM(COALESCE(rp.aprs_ret_ley_20630,0) + COALESCE(rp.aprs_ret_ley_23351,0) + COALESCE(rp.prs_ret_ley_20630,0) + COALESCE(rp.prs_ret_ley_23351,0)) AS retenciones_gan				
				FROM pre_resumen_emision rp
				JOIN sor_pgmsorteo ps ON ps.id = rp.sor_pgmsorteo_id_c  
				WHERE (COALESCE(rp.aprs_ret_ley_20630,0) + COALESCE(rp.aprs_ret_ley_23351,0) + COALESCE(rp.prs_ret_ley_20630,0) + COALESCE(rp.prs_ret_ley_23351,0)) != 0			   
							AND rp.pre_canal_de_pago_id_c = 'O' AND ps.id IN (", @param_lst_str_ids ,")
				GROUP BY ps.id, rp.tbl_provincias_id_c			
			) AS presc ON presc.tbl_provincias_id_c = c.tbl_provincias_id_c
			INNER JOIN (
			SELECT  rp.tbl_provincias_id_c,  
					SUM(COALESCE(rp.aprs_ret_ley_20630,0) + COALESCE(rp.aprs_ret_ley_23351,0) + COALESCE(rp.prs_ret_ley_20630,0) + COALESCE(rp.prs_ret_ley_23351,0)) AS retenciones_gan				
				FROM pre_resumen_emision rp
				JOIN sor_pgmsorteo ps ON ps.id = rp.sor_pgmsorteo_id_c  
				WHERE (COALESCE(rp.aprs_ret_ley_20630,0) + COALESCE(rp.aprs_ret_ley_23351,0) + COALESCE(rp.prs_ret_ley_20630,0) + COALESCE(rp.prs_ret_ley_23351,0)) != 0			   
							AND rp.pre_canal_de_pago_id_c = 'O' AND ps.id IN (", @param_lst_str_ids ,")
				GROUP BY rp.tbl_provincias_id_c	
			) tot_liq ON tot_liq.tbl_provincias_id_c = c.tbl_provincias_id_c
			WHERE a.categoria = 'provincia' AND a.estado = 'activo'
			  AND a.deleted = 0
			ORDER BY c.tbl_provincias_id_c,presc.id, presc.nombre_sorteo;")
	;
	PREPARE sentenciasql FROM @query;
	-- SELECT @query;
	EXECUTE sentenciasql;	
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - query: ', @query));
		
	-- lista de provincias a generar manuales por prescripcion
	manuales:BEGIN
		DECLARE Err_Cursor 		INT;
		DECLARE v_age_red_id_c 		CHAR(36);
		DECLARE v_nombre_red            CHAR(50);
		DECLARE v_tbl_provincias_id_c 	CHAR(36);
		DECLARE v_retenciones_gan	DECIMAL(18,2); 
		DECLARE v_id_nrosorteo 		CHAR(36);
		DECLARE v_nombre_sorteo         VARCHAR(255);
		DECLARE v_producto              VARCHAR(255);
		DECLARE v_lstNroSorteos         VARCHAR(4096);
		DECLARE v_provincia_anterior    CHAR(2); -- Variable para almacenar el valor anterior de tbl_provincias_id_c
		DECLARE v_total_liquidado	DECIMAL(18,2); 
		
		DECLARE pcias_emisiones CURSOR FOR		
		SELECT id_red,razon_social,provincia,retencion,id_sorteo,sorteo,producto,lstNroSorteos,total_liquidado
		FROM tmp_emisiones
		;
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET Err_Cursor = 1;	
		
		-- MARCAR LOS RESUMENES DE EMISION COMO GENERANDO NC (estado=3)
		SET @query2 = "";
		SET @query2 = CONCAT(@query2,"
			UPDATE pre_resumen_emision re			
			SET 	re.estado = '3', 				
				re.date_modified = @hoy
			WHERE re.sor_pgmsorteo_id_c IN (", @param_lst_str_ids ,");");
		PREPARE sentenciasql2 FROM @query2;
		-- select @query2;
		EXECUTE sentenciasql2;
		
		-- Inicializo varibale en null para primera iteracion
		SET v_provincia_anterior = NULL; 
		
		OPEN pcias_emisiones;
		manmovLoop: LOOP
			FETCH NEXT FROM pcias_emisiones 
				INTO v_age_red_id_c, v_nombre_red, v_tbl_provincias_id_c, v_retenciones_gan, v_id_nrosorteo, v_nombre_sorteo, v_producto, v_lstNroSorteos,v_total_liquidado;
						
			IF(Err_Cursor = 1) THEN
				SET Err_Cursor = 0;
				LEAVE manmovLoop;
			END IF;
			INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - cursor fetch - registro: v_age_red_id_c ->',v_age_red_id_c, 
					'<- v_nombre_red ->',v_nombre_red,
					'<- v_tbl_provincias_id_c ->',v_tbl_provincias_id_c,
					'<- v_retenciones_gan ->',v_retenciones_gan,
					'<- v_id_nrosorteo ->',v_id_nrosorteo,
					'<- v_total_liquidado ->',v_total_liquidado,
					'<- v_nombre_sorteo ->',v_nombre_sorteo)
			;
			
			-- recupera parametros de cuenta corriente. cod tipo mov. y cod afect. + idOperacion AGENTES - DB-CR VARIOS
			SET @tmov_pres_prov = 0, @afect_pres_prov = 0, @idOperacion = '';	
			SELECT  tmov_pres_prov, afect_pres_prov, id_op_pres_prov
			INTO @tmov_pres_prov, @afect_pres_prov, @idOperacion
			FROM cas02_cc_parametros;
				
			-- recupera idAgeRed, nombreRed
			SET @idAgeRed = v_age_red_id_c, @nombreRed = v_nombre_red, @importeLiquidado = v_retenciones_gan;
					
			-- set delegacion, grupo, fecha_carga
			SET @delegacion = '0', @grupo = 'P', @fecha_carga = CURDATE() ;
			
			-- datos para movimientos 
			SET @idTipoCbte = '', @idAfectacion = '', @fecha_ingreso = CURDATE(), @tipoDC = '';
			-- id tipo cbte, tipo_dc
			SELECT id, tipo_dc 
			INTO @idTipoCbte, @tipoDC
			FROM cas02_cc_tipos_comprobantes
			WHERE codigo = @tmov_pres_prov AND grupo = @grupo;
			-- id afectacion 
			SELECT id 
			INTO @idAfectacion
			FROM cas02_cc_afectaciones
			WHERE codigo_de_afectacion = @afect_pres_prov AND grupo = @grupo
			;
			-- Determino el cambio de provincia y genero una nueva manual		
			IF v_provincia_anterior IS NULL OR v_tbl_provincias_id_c <> v_provincia_anterior THEN
				
				-- CALL sor_inserta_auditoria(@id_proceso,juego, sorteo,84,usuario,84,85,CONCAT("NCs Pcias -     GENERA NC PARA ",v_nombre_red));
				-- llamar SP_ET_ManualesINS
				INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - Inicio CALL SP_ET_ManualesINS ******************');
				CALL `SP_ET_ManualesINS` (						
						par_idUsuario,  	
						@idOperacion,
						@idAgeRed,  	
						@delegacion, 		-- 0 = Santa fe
						@grupo, 		
						@fecha_carga, 
						CONCAT('Prescrip. ', v_producto, ' ', v_lstNroSorteos), 
						ABS(v_total_liquidado),  -- es la retencion que prescribe?
						0, 	
						@nombreRed,
						'',                   -- fecha valores
						@tipoDC,              -- tipo_dc	
						v_id_nrosorteo,       -- agregado
						0,                    -- agregado nro op	
						0,                    -- estado Corresponde a la version varias fechas valores que todavia no se instala
						@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt
				);
				-- Errores
				IF (@RCode <> 1) THEN
					-- ROLLBACK;
					INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - SP_ET_ManualesINS ', @RCode, ' - ', @RTxt, ' - ', @RSQLErrtxt);
					SET 	RCode      = @RCode,
						RTxt       = @RTxt,
						RId        = '',
						RSQLErrNo  = @RSQLErrNo,
						RSQLErrtxt = @RSQLErrtxt;
					LEAVE thisSP ;
				END IF;
							
				SET @idManual = @RId;
				INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - Fin CALL `SP_ET_ManualesINS` ',@idManual, '******************');
				
				-- Actualizo el valor de provincia_anterior con el valor actual de v_tbl_provincias_id_c
				SET v_provincia_anterior = v_tbl_provincias_id_c;
				INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - actualizo el valor de v_provincia_anterior: ', v_provincia_anterior);
			END IF;
			
			-- llamar SP_ET_MovimientosINS
			INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - Inicio CALL SP_ET_MovimientosINS ******************');
			CALL `SP_ET_MovimientosINS` (						
				@idManual,  	
				'',            -- idLiquidacion
				par_idUsuario,  	
				@idTipoCbte,   
				@idAfectacion, 		
				@idAgeRed, 				
				@idTipoCbte, 			
				@tmov_pres_prov,	
				ABS(@importeLiquidado), 	
				ABS(@importeLiquidado), 	
				@tipoDC,
				@fecha_ingreso,
				@delegacion,
				@grupo,
				0, -- intereses
				v_id_nrosorteo,       -- agregado
				v_tbl_provincias_id_c, -- agregado
				@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt
			);
			-- Errores
			IF (@RCode <> 1) THEN
				-- ROLLBACK;
				INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - SP_ET_MovimientosINS ', @RCode, ' - ', @RTxt, ' - ', @RSQLErrtxt);
				SET 	RCode      = @RCode,
					RTxt       = @RTxt,
					RId        = '',
					RSQLErrNo  = @RSQLErrNo,
					RSQLErrtxt = @RSQLErrtxt;
				LEAVE thisSP ;
			END IF;
					
			SET @idMovimiento = @RId;
			INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - Fin CALL `SP_ET_MovimientosINS` ',@idMovimiento, '******************');
			-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
					
			IF RCode <> 1 THEN 
				LEAVE thisSP;
			END IF;
								
		END LOOP manmovLoop;
		CLOSE pcias_emisiones;
	END;
	IF RCode = 1 THEN 
		-- MARCAR LOS RESUMENES DE EMISION COMO NC GENERADA (estado=4)
		SET @query3 = "";
		SET @query3 = CONCAT(@query3,"
			UPDATE pre_resumen_emision re			
			SET 	re.estado = '4', 				
				re.date_modified = @hoy
			WHERE re.sor_pgmsorteo_id_c IN (", @param_lst_str_ids ,");");
		PREPARE sentenciasql3 FROM @query3;
		-- select @query3;
		EXECUTE sentenciasql3;
		
		
		SET RTxt = 'Finalización correcta generacion NC prescriptos a provincias';
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' - CC_NC_Prescripcion_CtaCte_Provincia - Fin Proceso - Sorteo: ', @param_lst_str_ids));	
	END IF;
	-- COMMIT;
END$$

DELIMITER ;