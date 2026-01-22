DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_CIERRE_presc_emision`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_CIERRE_presc_emision`(
		IN id_pgmsorteo CHAR(36),
		IN id_ordenpago CHAR(36),
		IN id_consolidacion INT,
		IN opc INT,
		OUT msgerr VARCHAR(2000))
fin:BEGIN
/*	
-- opcion 1-> liquidacion de premios
-- opcion 2 -> pago de premios
-- opcion 3 -> prescripcion premios agencias
-- opcion 4 -> prescripcion premios por cierre de emision (OTRAS PROVINCIAS + TESORERIA)
*/	
--	DECLARE p_email, p_res VARCHAR(255) DEFAULT '';
	
	DECLARE mensaje VARCHAR(2048);
	DECLARE var_brincosueldo INT DEFAULT 2; 	-- PREMIO SUELDO --> PARA PREMIO BRINCO SUELDO
	DECLARE var_brinco INT DEFAULT 13; 		-- BRINCO --> PARA TRATAMIENTO PREMIO ESTIMULO Y CUOTAS
	DECLARE var_santafe CHAR(2) DEFAULT 'S';        -- PROVINCIA SANTA FE

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1
			@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
			@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???

		INSERT INTO kk_auditoria2(nombre) 
		VALUES(CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN CON ERRORES - id_pgmsorteo ', id_pgmsorteo, 
					' id_consolidacion ', id_consolidacion, 
					' - Error ', COALESCE(@errno, 0), ' Mensaje ', COALESCE(@msg, '')))
		;
		UPDATE tmp_estado_proc_cierre_emis_presc 
		SET     estadoproc = 'ERR - Excepción SQL en SP PREMIOS_CIERRE_presc_emision', 
			cod_estado = '9', 
			nro_sorteo = @sorteo, 
			juego = @juego, 
			date_modified = NOW()
		WHERE id_sorteo = @id_pgmsorteo 
		  AND idproceso = @id_consolidacion
		;
		SET msgerr = '9-ERR - Excepción SQL en SP PREMIOS_CIERRE_presc_emision';
		-- actualizo a estado original para ser procesado luego siendo en ese momento cuando cumpla con las condiciones
		UPDATE sor_pgmsorteo 
		SET     sor_emision_cerrada = 0,
			sor_fechor_emision_cerrada =  NULL 
		WHERE id = @id_pgmsorteo
		;
	END;

	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - INI - ID SORTEO: ', id_pgmsorteo, ' - ID ORDEN PAGO: ', id_ordenpago, ' id_consolidacion: ', id_consolidacion, ' opc: ', opc));

	SET @id_pgmsorteo = id_pgmsorteo;
	SET @opc = opc;
	SET @id_ordenpago = id_ordenpago;
	SET @id_consolidacion=id_consolidacion;
	SET @cant_conf_pend = '';
	SET @cant_resto = '';
	SET @cant_premios = '';
	SET @presc_recibida_boldt = '';
	SET @hoy = NOW();
	
	SET @sorteo ='';
	SET @juego ='';
	SET @proceso_consolida='';
	SET @fecha_proc='';
	SET @sor_emision_cerrada='';
	SET @sor_fechor_emision_cerrada='';
	SET @idestado = '';
	
	
	DROP TEMPORARY TABLE IF EXISTS tmp_pre_resumen_emision_vuelco; 
	CREATE TEMPORARY TABLE `tmp_pre_resumen_emision_vuelco` (
	  `id` CHAR(36) NOT NULL,
	  `name` VARCHAR(255) DEFAULT NULL,
	  `date_entered` DATETIME DEFAULT NULL,
	  `date_modified` DATETIME DEFAULT NULL,
	  `modified_user_id` CHAR(36) DEFAULT NULL,
	  `deleted` TINYINT(1) DEFAULT '0',
	  `sor_pgmsorteo_id_c` CHAR(36) DEFAULT NULL,
	  `tbl_provincias_id_c` CHAR(36) DEFAULT NULL,
	  `pre_canal_de_pago_id_c` CHAR(36) DEFAULT NULL,
	  `liq_pre_bruto` DECIMAL(18,2) DEFAULT NULL,
	  `liq_pre_neto` DECIMAL(18,2) DEFAULT NULL,
	  `liq_ret_total_sin_cudaio` DECIMAL(18,2) DEFAULT NULL,
	  `liq_ret_ley_20630` DECIMAL(18,2) DEFAULT NULL,
	  `liq_ret_ley_23351` DECIMAL(18,2) DEFAULT NULL,
	  `liq_ret_ley_11265` DECIMAL(18,2) DEFAULT NULL,
	  `liq_cudaio` DECIMAL(18,2) DEFAULT NULL,
	  `pag_pre_bruto` DECIMAL(18,2) DEFAULT NULL,
	  `pag_pre_neto` DECIMAL(18,2) DEFAULT NULL,
	  `pag_ret_total_sin_cudaio` DECIMAL(18,2) DEFAULT NULL,
	  `pag_ret_ley_20630` DECIMAL(18,2) DEFAULT NULL,
	  `pag_ret_ley_23351` DECIMAL(18,2) DEFAULT NULL,
	  `pag_ret_ley_11265` DECIMAL(18,2) DEFAULT NULL,
	  `pag_cudaio` DECIMAL(18,2) DEFAULT NULL,
	  `prs_pre_bruto` DECIMAL(18,2) DEFAULT NULL,
	  `prs_pre_neto` DECIMAL(18,2) DEFAULT NULL,
	  `prs_ret_total_sin_cudaio` DECIMAL(18,2) DEFAULT NULL,
	  `prs_ret_ley_20630` DECIMAL(18,2) DEFAULT NULL,
	  `prs_ret_ley_23351` DECIMAL(18,2) DEFAULT NULL,
	  `prs_ret_ley_11265` DECIMAL(18,2) DEFAULT NULL,
	  `prs_cudaio` DECIMAL(18,2) DEFAULT NULL,
	  PRIMARY KEY (`id`)
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
		
	SET @emisionCerrada = 0;
	
	SELECT COALESCE(sor_emision_cerrada,0), idjuego INTO @emisionCerrada , @juego  
	FROM sor_pgmsorteo WHERE id = @id_pgmsorteo
	; 
	IF (@emisionCerrada = 1) THEN -- 0 = no cerrada, 1 = cerrada
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN: Emisión ya cerrada. Verifique. ID SORTEO: ', id_pgmsorteo, ' - ID PRO CONSOLIDACION: id_consolidacion ', id_consolidacion));
		
		SET msgerr = 'Emisión ya cerrada. Verifique.' ;
		LEAVE fin;
	END IF;
	
	-- opcion 1-> liquidacion de premios
	IF (@opc = 1 ) THEN
		-- existe id sorteo en condiciones para procesar el cierre?
		IF EXISTS(
			SELECT *  FROM sor_pgmsorteo p 
			WHERE       p.deleted = 0 
				AND p.idestado = 50
				AND p.id = @id_pgmsorteo
			)
		THEN
			-- actualizo estado y fecha tomandolo para procesar
			-- 1. update de tabla sor_pgmsorteo estado 0 a 5 -> tomado
			--  UPDATE sor_pgmsorteo SET sor_emision_cerrada=5 WHERE id= @id_pgmsorteo;
			SELECT pg.nrosorteo, pg.idjuego 
				INTO @sorteo,@juego
			FROM sor_pgmsorteo pg 
			WHERE  pg.id = @id_pgmsorteo
			;
		ELSE 
			-- INSERT INTO tmp_estado_proc_cierre_emis_presc(id_sorteo,nro_sorteo,juego,estadoproc,cod_estado,idproceso,date_entered,date_modified)
			-- SELECT pg.id, pg.nrosorteo, pg.idjuego, NULL, NULL, @id_consolidacion,NOW(),NOW() FROM sor_pgmsorteo pg WHERE pg.id = @id_pgmsorteo;
			SET @estado = -1;
			SELECT pg.nrosorteo, pg.idjuego, pg.idestado
				INTO @sorteo,@juego, @estado
				FROM sor_pgmsorteo pg 
				WHERE  pg.id = @id_pgmsorteo
			;
			IF (@idestado = -1 )  THEN 
				SET msgerr = 'Resumen Contable de liquidacion de sorteo - No existe el sorteo.'; 
				LEAVE fin;
			ELSE 
				SET msgerr = 'Resumen Contable de liquidacion de sorteo - El sorteo no esta PUBLICADO'; 
				LEAVE fin;
			END IF;
		END IF;
		/*
		-- inserto todo en la temporal
		DROP TEMPORARY TABLE IF EXISTS tmp_tot;
		CREATE TEMPORARY TABLE tmp_tot (
		  sor_pgmsorteo_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  tbl_provincias_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  pre_canal_de_pago_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  origen VARCHAR(20) CHARACTER SET utf8 NOT NULL,
		  pag_pre_bruto DECIMAL(18,2) DEFAULT NULL,
		  pag_pre_neto DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_total_sin_cudaio DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_20630 DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_23351 DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_11265 DECIMAL(18,2) DEFAULT NULL,
		  pag_cudaio DECIMAL(18,2) DEFAULT NULL,
		  PRIMARY KEY (sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,origen)
		) ENGINE=INNODB
		;
		START TRANSACTION;
		-- premios 
		INSERT INTO tmp_tot
			SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'pre' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
			WHERE s.id= @id_pgmsorteo AND s.deleted = 0 
			GROUP BY s.id, pre.tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
					ELSE 'NI' 
				END
		;
		-- premios menores
		INSERT INTO tmp_tot
			SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'premen' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN pre_premios_menores pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
			WHERE s.id= @id_pgmsorteo AND s.deleted = 0 
			GROUP BY s.id, pre.tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
					ELSE 'NI' 
				END
		;
		-- premios menores historicos
		INSERT INTO tmp_tot
			SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'premenH' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN pre_premios_menores_historicos pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
			WHERE s.id= @id_pgmsorteo AND s.deleted = 0 
			GROUP BY s.id, pre.tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
					ELSE 'NI' 
				END
		;		
		-- premios cuotas, SOLO brinco!!!
		INSERT INTO tmp_tot
			SELECT  	s.id AS sor_pgmsorteo_id_c, 
					pre.tbl_provincias_id_c AS tbl_provincias_id_c,
					CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
						WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
						WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
						ELSE 'NI' 
					END AS canal, 
					'precuo' AS origen,
					0 AS pag_pre_bruto,
					SUM(pc.prc_neto) AS pag_pre_neto,
					0 AS pag_ret_total_sin_cudaio,
					0 AS pag_ret_ley_20630,
					0 AS pag_ret_ley_23351,
					0 AS pag_ret_ley_11265,
					0 AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN pre_premios_cuotas pc ON pc.sor_pgmsorteo_id_c = s.id AND pc.deleted = 0
				INNER JOIN pre_premios pre ON pre.id = pc.pre_premios_id_c AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
			WHERE  s.deleted = 0 
				AND s.id = @id_pgmsorteo 
				and @juego = var_brinco
			GROUP BY s.id, pre.tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
					WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
					ELSE 'NI' 
				END
		;
		-- actualizo los que existen
		UPDATE pre_resumen_emision r
			INNER JOIN (	SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
						SUM(pre.pag_pre_bruto) AS pag_pre_bruto,
						SUM(pre.pag_pre_neto) AS pag_pre_neto,
						SUM(pre.pag_ret_total_sin_cudaio) AS pag_ret_total_sin_cudaio,
						SUM(pre.pag_ret_ley_20630) AS pag_ret_ley_20630,
						SUM(pre.pag_ret_ley_23351) AS pag_ret_ley_23351,
						SUM(pre.pag_ret_ley_11265) AS pag_ret_ley_11265,
						SUM(pre.pag_cudaio) AS pag_cudaio
					FROM tmp_tot pre
					GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
				   ) t ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c 
						AND r.tbl_provincias_id_c = t.tbl_provincias_id_c 
						AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
			SET r.date_modified = NOW(),
			    r.liq_pre_bruto = t.pag_pre_bruto,
			    r.liq_pre_neto = t.pag_pre_neto,
			    r.liq_ret_total_sin_cudaio = t.pag_ret_total_sin_cudaio,
			    r.liq_ret_ley_20630 = t.pag_ret_ley_20630,
			    r.liq_ret_ley_23351 = t.pag_ret_ley_23351,
			    r.liq_ret_ley_11265 = t.pag_ret_ley_11265,
			    r.liq_cudaio = t.pag_cudaio
		;
		-- inserto los que no existen
		INSERT INTO pre_resumen_emision (id,NAME,date_entered,date_modified,modified_user_id,created_by,description,deleted,assigned_user_id,sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,liq_pre_bruto,liq_pre_neto,liq_ret_total_sin_cudaio,liq_ret_ley_20630,liq_ret_ley_23351,liq_ret_ley_11265,liq_cudaio,pag_pre_bruto,pag_pre_neto,pag_ret_total_sin_cudaio,pag_ret_ley_20630,pag_ret_ley_23351,pag_ret_ley_11265,pag_cudaio,prs_pre_bruto,prs_pre_neto,prs_ret_total_sin_cudaio,prs_ret_ley_20630,prs_ret_ley_23351,prs_ret_ley_11265,prs_cudaio,prs_pre_bruto_mens,prs_pre_neto_mens,prs_ret_total_mens,prs_ret_ley20630_mens,prs_ret_ley23351_mens)
			SELECT
				UUID() AS id,
				CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', CASE WHEN t.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS NAME,
				NOW() AS date_entered,
				NOW() AS date_modified,
				'1' AS modified_user_id,
				'1' AS created_by,
				CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', CASE WHEN t.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS description,
				0 AS deleted,
				'1' AS assigned_user_id,
				
				t.sor_pgmsorteo_id_c, 
				t.tbl_provincias_id_c AS tbl_provincias_id_c,
				t.pre_canal_de_pago_id_c, 
				
				t.pag_pre_bruto,
				t.pag_pre_neto,
				t.pag_ret_total_sin_cudaio,
				t.pag_ret_ley_20630,
				t.pag_ret_ley_23351,
				t.pag_ret_ley_11265,
				t.pag_cudaio, 
				0 AS pag_pre_bruto,
				0 AS pag_pre_neto,
				0 AS pag_ret_total_sin_cudaio,
				0 AS pag_ret_ley_20630,
				0 AS pag_ret_ley_23351,
				0 AS pag_ret_ley_11265,
				0 AS pag_cudaio,
				0 AS prs_pre_bruto,
				0 AS prs_pre_neto,
				0 AS prs_ret_total_sin_cudaio,
				0 AS prs_ret_ley_20630,
				0 AS prs_ret_ley_23351,
				0 AS prs_ret_ley_11265,
				0 AS prs_cudaio,
				
				0 AS prs_pre_bruto_mens,
				0 AS prs_pre_neto_mens,
				0 AS prs_ret_total_mens,
				0 AS prs_ret_ley_20630_mens,
				0 AS prs_ret_ley_23351_mens
			FROM  
			(
			SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
				SUM(pre.pag_pre_bruto) AS pag_pre_bruto,
				SUM(pre.pag_pre_neto) AS pag_pre_neto,
				SUM(pre.pag_ret_total_sin_cudaio) AS pag_ret_total_sin_cudaio,
				SUM(pre.pag_ret_ley_20630) AS pag_ret_ley_20630,
				SUM(pre.pag_ret_ley_23351) AS pag_ret_ley_23351,
				SUM(pre.pag_ret_ley_11265) AS pag_ret_ley_11265,
				SUM(pre.pag_cudaio) AS pag_cudaio
			FROM tmp_tot pre
			GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
			) t
				INNER JOIN sor_pgmsorteo s ON s.id = t.sor_pgmsorteo_id_c AND s.deleted=0
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = t.tbl_provincias_id_c AND pro.deleted = 0
				LEFT JOIN  pre_resumen_emision r ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
			WHERE r.sor_pgmsorteo_id_c IS NULL
		;
		COMMIT;
		
		DROP TEMPORARY TABLE IF EXISTS tmp_tot;
		*/

		-- Se rehace la tabla pre_resumen_emision como solución a que no de todos los puntos donde se actualizan estados de premios se actualiza el resumen,
		--   y resulta menos costoso en impacto y tiempo hacerlo de esta manera.
		CALL PREMIOS_CIERRE_recalcula_resumen_emision(@id_pgmsorteo,0, @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
	
		SET msgerr = 'OK'; 
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN OK - ID SORTEO: ', id_pgmsorteo, ' - ID ORDEN PAGO: ', id_ordenpago, ' id_consolidacion: ', id_consolidacion, ' opc: ', opc));
		
		LEAVE fin;
		
	END IF;
	
	-- opcion 2 -> pago de premios del procesode resumen de emision 
	-- esta opcion recalcula los totales pagados por cada canal de pago para el idpgmsorteo.
	IF (@opc = 2 ) THEN
		IF EXISTS(
			-- sorteo publicado y no está cerrado
			SELECT *  
			FROM sor_pgmsorteo p 
			WHERE       p.deleted = 0 
				AND p.idestado = 50 
				AND COALESCE(sor_emision_cerrada,0) = 0
				AND p.id = @id_pgmsorteo 
			 )
		THEN
			-- si existe 
			SELECT pg.nrosorteo, pg.idjuego 
				INTO @sorteo,@juego
			FROM sor_pgmsorteo pg 
			WHERE  pg.id = @id_pgmsorteo
			;
		ELSE
			SET @estado = -1;
			
			SELECT 	pg.nrosorteo, pg.idjuego, pg.idestado, pg.sor_emision_cerrada
				INTO @sorteo, @juego, @estado, @cerrado
			FROM sor_pgmsorteo pg 
			WHERE  pg.id = @id_pgmsorteo
			;
		
			IF (@idestado = -1 )  THEN 
				SET msgerr = 'Resumen emision - No existe el sorteo.'; 
				LEAVE fin;
			END IF;	
		
			IF (@idestado <> 50 )  THEN 
				SET msgerr = 'Resumen emision - El sorteo no tiene estado PUBLICADO'; 
				LEAVE fin;
			END IF;	
			
			IF ( @cerrado = 1 ) THEN
				SET msgerr = 'Resumen emision - El sorteo tiene emision CERRADA'; 
				LEAVE fin;
			END IF;	
		END IF;
		/*
		-- inserto todo en la temporal
		DROP TEMPORARY TABLE IF EXISTS tmp_tot;
		CREATE TEMPORARY TABLE tmp_tot (
		  sor_pgmsorteo_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  tbl_provincias_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  pre_canal_de_pago_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  origen VARCHAR(20) CHARACTER SET utf8 NOT NULL,
		  pag_pre_bruto DECIMAL(18,2) DEFAULT NULL,
		  pag_pre_neto DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_total_sin_cudaio DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_20630 DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_23351 DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_11265 DECIMAL(18,2) DEFAULT NULL,
		  pag_cudaio DECIMAL(18,2) DEFAULT NULL,
		  PRIMARY KEY (sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,origen)
		) ENGINE=INNODB
		;
		START TRANSACTION;
		-- premios 
		INSERT INTO tmp_tot
		SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				COALESCE(op.pre_canal_de_pago_id_c,'A') AS pre_canal_de_pago_id_c, -- si el premio esta pagado y la op no tiene el canal de pago es porque no esta registrado el beneficiario, ergo es un premio pagado en agencia para el cual el archivo de pagos se proceso antes del beneficiario
				'pre' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
		FROM sor_pgmsorteo s
		INNER JOIN sor_producto p ON p.id_juego  = s.idjuego AND p.deleted = 0
		INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
		INNER JOIN pre_orden_pago op ON op.pre_premios_id_c = pre.id AND op.deleted = 0
		INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.id= @id_pgmsorteo AND s.deleted = 0 
		AND pre.pre_estadopago = 'A' 
		GROUP BY s.id, pre.tbl_provincias_id_c, COALESCE(op.pre_canal_de_pago_id_c,'A')
		;
		-- premios_menores
		INSERT INTO tmp_tot
		SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
				ELSE 'NI' END 		
				AS pre_canal_de_pago_id_c, 
				'premen' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
		FROM sor_pgmsorteo s
		INNER JOIN sor_producto p ON p.id_juego  = s.idjuego AND p.deleted = 0
		INNER JOIN pre_premios_menores pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
		INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.id= @id_pgmsorteo AND s.deleted = 0 
		AND pre.pre_estadopago = 'A' 
		GROUP BY s.id, pre.tbl_provincias_id_c, 
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
				ELSE 'NI' 
			END
		;
		-- premios_menores_historicos
		INSERT INTO tmp_tot
		SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
				ELSE 'NI' END 		
				AS pre_canal_de_pago_id_c, 
				'premenH' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
		FROM sor_pgmsorteo s
		INNER JOIN sor_producto p ON p.id_juego  = s.idjuego AND p.deleted = 0
		INNER JOIN pre_premios_menores_historicos pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
		INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.id= @id_pgmsorteo AND s.deleted = 0 
		AND pre.pre_estadopago = 'A' 
		GROUP BY s.id, pre.tbl_provincias_id_c, 
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
				WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
				ELSE 'NI' 
			END
		;		
		-- actualizo los que existen
		UPDATE pre_resumen_emision r
		INNER JOIN (
		SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
			SUM(pre.pag_pre_bruto) AS pag_pre_bruto,
			SUM(pre.pag_pre_neto) AS pag_pre_neto,
			SUM(pre.pag_ret_total_sin_cudaio) AS pag_ret_total_sin_cudaio,
			SUM(pre.pag_ret_ley_20630) AS pag_ret_ley_20630,
			SUM(pre.pag_ret_ley_23351) AS pag_ret_ley_23351,
			SUM(pre.pag_ret_ley_11265) AS pag_ret_ley_11265,
			SUM(pre.pag_cudaio) AS pag_cudaio
		FROM tmp_tot pre
		GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
		) t ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
		SET r.date_modified = NOW(),
		    r.pag_pre_bruto = t.pag_pre_bruto,
		    r.pag_pre_neto = t.pag_pre_neto,
		    r.pag_ret_total_sin_cudaio = t.pag_ret_total_sin_cudaio,
		    r.pag_ret_ley_20630 = t.pag_ret_ley_20630,
		    r.pag_ret_ley_23351 = t.pag_ret_ley_23351,
		    r.pag_ret_ley_11265 = t.pag_ret_ley_11265,
		    r.pag_cudaio = t.pag_cudaio
		;
		-- inserto los que no existen
		INSERT INTO pre_resumen_emision (id,NAME,date_entered,date_modified,modified_user_id,created_by,description,deleted,assigned_user_id,sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,liq_pre_bruto,liq_pre_neto,liq_ret_total_sin_cudaio,liq_ret_ley_20630,liq_ret_ley_23351,liq_ret_ley_11265,liq_cudaio,pag_pre_bruto,pag_pre_neto,pag_ret_total_sin_cudaio,pag_ret_ley_20630,pag_ret_ley_23351,pag_ret_ley_11265,pag_cudaio,prs_pre_bruto,prs_pre_neto,prs_ret_total_sin_cudaio,prs_ret_ley_20630,prs_ret_ley_23351,prs_ret_ley_11265,prs_cudaio,prs_pre_bruto_mens,prs_pre_neto_mens,prs_ret_total_mens,prs_ret_ley20630_mens,prs_ret_ley23351_mens)
		SELECT
			UUID() AS id,
			CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', CASE WHEN t.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS NAME,
			NOW() AS date_entered,
			NOW() AS date_modified,
			'1' AS modified_user_id,
			'1' AS created_by,
			CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', CASE WHEN t.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS description,
			0 AS deleted,
			'1' AS assigned_user_id,
			t.sor_pgmsorteo_id_c, 
			t.tbl_provincias_id_c AS tbl_provincias_id_c,
			t.pre_canal_de_pago_id_c, 
			0 AS liq_pre_bruto,
			0 AS liq_pre_neto,
			0 AS liq_ret_total_sin_cudaio,
			0 AS liq_ret_ley_20630,
			0 AS liq_ret_ley_23351,
			0 AS liq_ret_ley_11265,
			0 AS liq_cudaio,
			t.pag_pre_bruto,
			t.pag_pre_neto,
			t.pag_ret_total_sin_cudaio,
			t.pag_ret_ley_20630,
			t.pag_ret_ley_23351,
			t.pag_ret_ley_11265,
			t.pag_cudaio, 
			0 AS prs_pre_bruto,
			0 AS prs_pre_neto,
			0 AS prs_ret_total_sin_cudaio,
			0 AS prs_ret_ley_20630,
			0 AS prs_ret_ley_23351,
			0 AS prs_ret_ley_11265,
			0 AS prs_cudaio,
			0 AS prs_pre_bruto_mens,
			0 AS prs_pre_neto_mens,
			0 AS prs_ret_total_mens,
			0 AS prs_ret_ley_20630_mens,
			0 AS prs_ret_ley_23351_mens
		FROM  
		(
		SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
			SUM(pre.pag_pre_bruto) AS pag_pre_bruto,
			SUM(pre.pag_pre_neto) AS pag_pre_neto,
			SUM(pre.pag_ret_total_sin_cudaio) AS pag_ret_total_sin_cudaio,
			SUM(pre.pag_ret_ley_20630) AS pag_ret_ley_20630,
			SUM(pre.pag_ret_ley_23351) AS pag_ret_ley_23351,
			SUM(pre.pag_ret_ley_11265) AS pag_ret_ley_11265,
			SUM(pre.pag_cudaio) AS pag_cudaio
		FROM tmp_tot pre
		GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
		) t
		INNER JOIN sor_pgmsorteo s ON s.id = t.sor_pgmsorteo_id_c AND s.deleted=0
		INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
		INNER JOIN tbl_provincias pro ON pro.id = t.tbl_provincias_id_c AND pro.deleted = 0
		LEFT JOIN  pre_resumen_emision r ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
		WHERE r.sor_pgmsorteo_id_c IS NULL
		;
		COMMIT;
		
		DROP TEMPORARY TABLE IF EXISTS tmp_tot;
		*/
		
		-- Se rehace la tabla pre_resumen_emision como solución a que no de todos los puntos donde se actualizan estados de premios se actualiza el resumen,
		--   y resulta menos costoso en impacto y tiempo hacerlo de esta manera.
		CALL PREMIOS_CIERRE_recalcula_resumen_emision(@id_pgmsorteo,0, @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		
		SET msgerr = 'OK'; 
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN OK - ID SORTEO: ', id_pgmsorteo, ' - ORDEN PAGO: ', id_ordenpago, ' id_consolidacion: ', id_consolidacion, ' opc: ', opc));
		
		LEAVE fin;
	
	
	END IF;
	
	-- opcion 3 -> resumen de prescriptos
	IF (@opc = 3 ) THEN
		IF EXISTS(
			SELECT *  
			FROM sor_pgmsorteo p 
			WHERE p.id = @id_pgmsorteo 
			  AND p.sor_emision_cerrada=0 
			  AND p.sor_fechor_emision_cerrada IS NULL
			)
		THEN
			-- actualizo estado y fecha tomandolo para procesar
			-- 1. update de tabla sor_pgmsorteo estado 0 a 5 -> tomado
			-- UPDATE sor_pgmsorteo SET sor_emision_cerrada=5 WHERE id= @id_pgmsorteo;
			
			SELECT pg.nrosorteo, pg.idjuego INTO @sorteo,@juego
				FROM sor_pgmsorteo pg 
				WHERE  pg.id=@id_pgmsorteo;
			
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - CIERRE_presc_emision - proceso ', @id_pgmsorteo, ' juego ', @juego, ' sorteo ', @sorteo));
		ELSE 
			SELECT pg.nrosorteo, pg.idjuego, pg.sor_fechor_emision_cerrada, pg.idestado 
					INTO @sorteo,@juego, @sor_fechor_emision_cerrada, @idestado
				FROM sor_pgmsorteo pg 
				WHERE  pg.id = @id_pgmsorteo
			;
			-- si hace falta otra validacion inicial (estado de sorteo campo idestado 50 -> publicado, no aparece en las reglas)
			IF (@idestado != 50)  THEN 
				SET msgerr = 'El sorteo no se encuentra en estado publicado'; 
				LEAVE fin;
			END IF;
		END IF;

		-- valido No existen premios con Estado de Registro de Beneficiario CONFIRMADO y Estado de Pago PENDIENTE.
		
		-- **************************************************   TODO OK - COMIENZA EL PROCESO *************************************************
		/*
		-- inserto todo en la temporal
		DROP TEMPORARY TABLE IF EXISTS tmp_tot;
		CREATE TEMPORARY TABLE tmp_tot (
		  sor_pgmsorteo_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  tbl_provincias_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  pre_canal_de_pago_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  origen VARCHAR(10) CHARACTER SET utf8 NOT NULL,
		  prs_pre_bruto DECIMAL(18,2) DEFAULT NULL,
		  prs_pre_neto DECIMAL(18,2) DEFAULT NULL,
		  prs_ret_total_sin_cudaio DECIMAL(18,2) DEFAULT NULL,
		  prs_ret_ley_20630 DECIMAL(18,2) DEFAULT NULL,
		  prs_ret_ley_23351 DECIMAL(18,2) DEFAULT NULL,
		  prs_ret_ley_11265 DECIMAL(18,2) DEFAULT NULL,
		  prs_cudaio DECIMAL(18,2) DEFAULT NULL,
		  PRIMARY KEY (sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,origen)
		) ENGINE=INNODB
		;
		
		-- inserto todo en la temporal
		INSERT INTO tmp_tot
			SELECT  pgm.id AS sor_pgmsorteo_id_c,
				var_santafe AS tbl_provincias_id_c,
				'A' AS pre_canal_de_pago_id_c, -- SOLO SE RECIBEN PRESCRIPCIONES DE TIPO AGENCIA!
				'afect' AS origen,             -- UN ÚNICO ORIGEN!!!
				SUM(CASE WHEN cc.co_id IN (15,25,30) THEN det.ad_importe ELSE 0 END) AS prs_pre_bruto,
				SUM(CASE WHEN cc.co_id IN (15) THEN det.ad_importe ELSE 0 END) AS prs_pre_neto,
				SUM(CASE WHEN cc.co_id IN (25) THEN det.ad_importe ELSE 0 END) AS prs_ret_total_sin_cudaio,
				0 AS prs_ret_ley_20630,
				0 AS prs_ret_ley_23351,
				SUM(CASE WHEN cc.co_id IN (30) THEN det.ad_importe ELSE 0 END) AS prs_ret_ley_11265,
				SUM(CASE WHEN cc.co_id IN (30) THEN det.ad_importe ELSE 0 END) AS prs_cudaio
			FROM cas02_cc_prescripcion_cab cab
				INNER JOIN cas02_cc_prescripcion_det_c detc ON detc.cas02_cc_ab25bion_cab_ida = cab.id AND detc.deleted = 0
				INNER JOIN cas02_cc_prescripcion_det det ON det.id = detc.cas02_cc_a3529ion_det_idb AND det.deleted = 0
				INNER JOIN cas02_cc_conceptos cc ON cc.co_id = cas02_cc_conceptos_id_c AND cc.deleted = 0
				INNER JOIN sor_pgmsorteo pgm ON pgm.`sor_producto_id_c` = cab.cas02_cc_juegos_id_c AND pgm.nrosorteo = cab.ac_sorteo
			WHERE pgm.id= @id_pgmsorteo AND cab.deleted = 0;
		
		-- sobre la temporal realizo la consulta if not exist
		-- genera resumen  de prescripcion
		
		-- UNICO REGISTRO! 1 SORTEO, 1 PROVINCIA 1 CANAL
		UPDATE pre_resumen_emision re
			INNER JOIN tmp_tot se ON se.sor_pgmsorteo_id_c = re.sor_pgmsorteo_id_c 
						AND  se.tbl_provincias_id_c = re.tbl_provincias_id_c 
						AND  se.pre_canal_de_pago_id_c = re.pre_canal_de_pago_id_c 
				
				SET 	re.date_modified = NOW(),
					re.prs_pre_bruto = se.prs_pre_bruto, 
					re.prs_pre_neto = se.prs_pre_neto, 
					re.prs_ret_total_sin_cudaio = se.prs_ret_total_sin_cudaio, 
					re.prs_ret_ley_20630 = se.prs_ret_ley_20630, 
					re.prs_ret_ley_23351 = se.prs_ret_ley_23351,
					re.prs_ret_ley_11265 = se.prs_ret_ley_11265,
					re.prs_cudaio = se.prs_cudaio
				;
		INSERT INTO pre_resumen_emision (id,NAME,date_entered,date_modified,modified_user_id,created_by,description,deleted,assigned_user_id,
							sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,
							liq_pre_bruto, liq_pre_neto, liq_ret_total_sin_cudaio, liq_ret_ley_20630, liq_ret_ley_23351, liq_ret_ley_11265, liq_cudaio,
							pag_pre_bruto, pag_pre_neto, pag_ret_total_sin_cudaio, pag_ret_ley_20630, pag_ret_ley_23351, pag_ret_ley_11265, pag_cudaio,
							prs_pre_bruto, prs_pre_neto, prs_ret_total_sin_cudaio, prs_ret_ley_20630, prs_ret_ley_23351, prs_ret_ley_11265, prs_cudaio,
							prs_pre_bruto_mens, prs_pre_neto_mens, prs_ret_total_mens, prs_ret_ley20630_mens, prs_ret_ley23351_mens)
		SELECT
			UUID() AS id,
			CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', pro.name) AS `name`,
			NOW() AS date_entered,
			NOW() AS date_modified,
			'1' AS modified_user_id,
			'1' AS created_by,
			CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', pro.name) AS description,
			0 AS deleted,
			'1' AS assigned_user_id,
				
			t.sor_pgmsorteo_id_c, 
			t.tbl_provincias_id_c AS tbl_provincias_id_c,
			t.pre_canal_de_pago_id_c, 
			
			0 AS liq_pre_bruto,
			0 AS liq_pre_neto,
			0 AS liq_ret_total_sin_cudaio,
			0 AS liq_ret_ley_20630,
			0 AS liq_ret_ley_23351,
			0 AS liq_ret_ley_11265,
			0 AS liq_cudaio,
			
			0 AS pag_pre_bruto,
			0 AS pag_pre_neto,
			0 AS pag_ret_total_sin_cudaio,
			0 AS pag_ret_ley_20630,
			0 AS pag_ret_ley_23351,
			0 AS pag_ret_ley_11265,
			0 AS pag_cudaio,
		
			t.prs_pre_bruto,
			t.prs_pre_neto,
			t.prs_ret_total_sin_cudaio,
			t.prs_ret_ley_20630,
			t.prs_ret_ley_23351,
			t.prs_ret_ley_11265,
			t.prs_cudaio, 
			
			0 AS prs_pre_bruto_mens,
			0 AS prs_pre_neto_mens,
			0 AS prs_ret_total_mens,
			0 AS prs_ret_ley_20630_mens,
			0 AS prs_ret_ley_23351_mens
		FROM  tmp_tot t
				INNER JOIN sor_pgmsorteo s ON s.id = t.sor_pgmsorteo_id_c AND s.deleted=0
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = t.tbl_provincias_id_c AND pro.deleted = 0
				LEFT JOIN  pre_resumen_emision r ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
			WHERE r.sor_pgmsorteo_id_c IS NULL
		;
		*/
		-- Se rehace la tabla pre_resumen_emision como solución a que no de todos los puntos donde se actualizan estados de premios se actualiza el resumen,
		--   y resulta menos costoso en impacto y tiempo hacerlo de esta manera.
		-- CALL PREMIOS_CIERRE_recalcula_resumen_emision(@id_pgmsorteo,0, @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		-- --------------------------------------------------------------------------------------------------------
		
		-- actualizo a estado de premio a "PRESCRIPTO"
		UPDATE pre_premios prem 
			INNER JOIN sor_pgmsorteo ss ON ss.id = prem.sor_pgmsorteo_id_c AND ss.deleted = 0
			INNER JOIN sor_producto pr ON pr.id  = ss.`sor_producto_id_c` AND pr.deleted = 0
				
				SET prem.pre_estadopago = 'R', -- estado prescripto 
					prem.pre_prcpagpres = CONCAT('ppme_', @id_consolidacion),
					prem.date_modified  = @hoy
					
			WHERE prem.sor_pgmsorteo_id_c = @id_pgmsorteo
				AND prem.deleted = 0 
				AND prem.pre_pagaagencia = 'S'
				AND prem.tbl_provincias_id_c = var_santafe
				AND prem.pre_estadopago = 'E';
		-- actualizo a estado de premio menor a "PRESCRIPTO"
		UPDATE pre_premios_menores prem 
			INNER JOIN sor_pgmsorteo ss ON ss.id = prem.sor_pgmsorteo_id_c AND ss.deleted = 0
			INNER JOIN sor_producto pr ON pr.id  = ss.`sor_producto_id_c` AND pr.deleted = 0
				
				SET prem.pre_estadopago = 'R', -- estado prescripto 
					prem.pre_prcpagpres = CONCAT('ppme_', @id_consolidacion),
					prem.date_modified  = @hoy
				
			WHERE prem.sor_pgmsorteo_id_c = @id_pgmsorteo
				AND prem.deleted = 0 
				AND prem.pre_pagaagencia = 'S'
				AND prem.tbl_provincias_id_c = var_santafe
				AND prem.pre_estadopago = 'E';
		-- actualizo a estado de premio menor historico a "PRESCRIPTO"
		UPDATE pre_premios_menores_historicos prem 
			INNER JOIN sor_pgmsorteo ss ON ss.id = prem.sor_pgmsorteo_id_c AND ss.deleted = 0
			INNER JOIN sor_producto pr ON pr.id  = ss.`sor_producto_id_c` AND pr.deleted = 0
				
				SET prem.pre_estadopago = 'R', -- estado prescripto 
					prem.pre_prcpagpres = CONCAT('ppme_', @id_consolidacion),
					prem.date_modified  = @hoy
				
			WHERE prem.sor_pgmsorteo_id_c = @id_pgmsorteo
				AND prem.deleted = 0 
				AND prem.pre_pagaagencia = 'S'
				AND prem.tbl_provincias_id_c = var_santafe
				AND prem.pre_estadopago = 'E';
				
		SET @estado_premios = 0, @estado_premios_menores = 0, @estado_premios_menores_his = 0;
		SELECT SUM(CASE WHEN o.pre_estadopago != 'R' THEN 1 ELSE 0 END)  INTO @estado_premios
			FROM pre_premios o 
			WHERE o.sor_pgmsorteo_id_c = @id_pgmsorteo 
				AND o.tbl_provincias_id_c = var_santafe 
				AND o.pre_pagaagencia = 'S' ;
		SELECT SUM(CASE WHEN o.pre_estadopago != 'R' THEN 1 ELSE 0 END)  INTO @estado_premios_menores
			FROM pre_premios_menores o 
			WHERE o.sor_pgmsorteo_id_c = @id_pgmsorteo 
				AND o.tbl_provincias_id_c = var_santafe 
				AND o.pre_pagaagencia = 'S' ;
		SELECT SUM(CASE WHEN o.pre_estadopago != 'R' THEN 1 ELSE 0 END)  INTO @estado_premios_menores_his
			FROM pre_premios_menores o 
			WHERE o.sor_pgmsorteo_id_c = @id_pgmsorteo 
				AND o.tbl_provincias_id_c = var_santafe 
				AND o.pre_pagaagencia = 'S' ;
		IF ( (@estado_premios + @estado_premios_menores + @estado_premios_menores_his) = 0) THEN 
			-- actualizo a estado original para ser procesado luego siendo en ese momento cuando cumpla con las condiciones
			SET msgerr = CONCAT('No se actualizo estado de premio a prescripto. Premios:', @estado_premios, '-Premios Menores:', @estado_premios_menores ) ; -- no existe el id de sorteo 
			LEAVE fin;	
		END IF;
		
		UPDATE sor_pgmsorteo 
			SET     sor_presc_recibida = 1, 
				sor_fechor_presc_recibida = NOW()  
			WHERE id = @id_pgmsorteo
		;
		
		-- Se rehace la tabla pre_resumen_emision con los prescriptos
		CALL PREMIOS_CIERRE_recalcula_resumen_emision(@id_pgmsorteo,0, @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		-- --------------------------------------------------------------------------------------------------------
		
		SET msgerr = 'OK'; 
		
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN OK - ID SORTEO: ', id_pgmsorteo, ' ORDEN PAGO: ', id_ordenpago, ' id_consolidacion: ', id_consolidacion, ' opc: ', opc));
		
		LEAVE fin;
	END IF;
	
	-- opcion 4 -> prescripcion premios Tesorería y Provincias por cierre de emision
	IF (@opc = 4 ) THEN
		IF EXISTS(
			SELECT *  FROM sor_pgmsorteo p WHERE p.id = @id_pgmsorteo AND p.sor_emision_cerrada = 0 
			)
		THEN
			-- EXISTE Y NO ESTA CERRADA!
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - INICIO - LA EMISION EXISTE Y NO ESTA CERRADA, OK. '));
			
			DELETE FROM tmp_estado_proc_cierre_emis_presc 
			WHERE id_sorteo = @id_pgmsorteo;
			INSERT INTO tmp_estado_proc_cierre_emis_presc(id_sorteo,nro_sorteo,juego,estadoproc,cod_estado,idproceso,date_entered,date_modified)
				SELECT pg.id, pg.nrosorteo, pg.idjuego, NULL, NULL, @id_consolidacion,NOW(),NOW() FROM sor_pgmsorteo pg WHERE pg.id = @id_pgmsorteo;
		
			-- actualizo estado y fecha tomandolo para procesar
			-- 1. update de tabla sor_pgmsorteo estado 0 a 5 -> tomado
			-- UPDATE sor_pgmsorteo SET sor_emision_cerrada=5 WHERE id= @id_pgmsorteo;
			
			-- 2. recupero juego y sorteo
			SELECT pg.nrosorteo, pg.idjuego INTO @sorteo,@juego
				FROM sor_pgmsorteo pg 
				WHERE  pg.id = @id_pgmsorteo
			;
		ELSE 
			-- NO EXISTE o YA FUE CERRADA
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - INICIO - LA EMISION NO EXISTE o YA FUE CERRADA!! SE CANCELA. '));
			
			DELETE FROM tmp_estado_proc_cierre_emis_presc 
			WHERE id_sorteo = @id_pgmsorteo;
			INSERT INTO tmp_estado_proc_cierre_emis_presc(id_sorteo,nro_sorteo,juego,estadoproc,cod_estado,idproceso,date_entered,date_modified)
				SELECT pg.id, pg.nrosorteo, pg.idjuego, NULL, NULL, @id_consolidacion,NOW(),NOW() FROM sor_pgmsorteo pg WHERE pg.id = @id_pgmsorteo;
			
			-- Determino el mensaje a enviar!
			SELECT pg.nrosorteo, pg.idjuego, pg.sor_emision_cerrada ,pg.sor_fechor_emision_cerrada ,pg.idestado
					INTO @sorteo,@juego, @sor_emision_cerrada, @sor_fechor_emision_cerrada, @idestado
			FROM sor_pgmsorteo pg 
			WHERE  pg.id = @id_pgmsorteo
			;
			-- aqui se define el dato puntual y error
			IF (@sor_emision_cerrada != 0) THEN 
				UPDATE tmp_estado_proc_cierre_emis_presc 
				SET estadoproc='ERR - El id_pgmsorteo se encuentra con emision cerrada',
					cod_estado = '2',
					nro_sorteo = @sorteo,
					juego = @juego,
					date_modified = NOW()
				WHERE       id_sorteo = @id_pgmsorteo 
					AND idproceso = @id_consolidacion
				; 
				SET msgerr = '2-ERR'; 
				INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - Validación no superada: ',msgerr));
				LEAVE fin;
			END IF;
			
			-- si hace falta otra validacion inicial (estado de sorteo campo idestado 50 -> publicado, no aparece en las reglas)
			IF (@idestado != 50)  THEN 
				UPDATE tmp_estado_proc_cierre_emis_presc 
				SET     estadoproc = 'ERR - El sorteo no se encuentra en estado publicado',
					cod_estado = '3',
					nro_sorteo = @sorteo,
					juego = @juego,
					date_modified = NOW()
				WHERE       id_sorteo = @id_pgmsorteo 
					AND idproceso = @id_consolidacion
				; 
				SET msgerr = '3-ERR'; 
				INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - Validación no superada: ',msgerr));
				LEAVE fin;
			END IF;
			-- Error por defecto
			UPDATE tmp_estado_proc_cierre_emis_presc 
			SET     estadoproc = 'ERR - El sorteo no existe o ya se encuentra cerrado',
				cod_estado = '99-ERR',
				nro_sorteo = @sorteo,
				juego = @juego,
				date_modified = NOW()
			WHERE       id_sorteo = @id_pgmsorteo 
				AND idproceso = @id_consolidacion
			; 
			SET msgerr = '99-ERR'; 			
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - LA EMISION NO EXISTE o YA FUE CERRADA!! SE CANCELA. '));
			LEAVE fin;
			
		END IF;
		
		-- valido No existen premios con Estado de Registro de Beneficiario CONFIRMADO y Estado de Pago PENDIENTE.
		SELECT  	
			COALESCE(SUM(CASE WHEN (pre.pre_estregistrobenef = 'C' AND pre.pre_estadopago = 'E') THEN 1 ELSE 0 END), 0) AS cant_conf_pend,
			COALESCE((COUNT(*) - SUM(CASE WHEN (pre.pre_estregistrobenef = 'C' AND  pre.pre_estadopago = 'E') THEN 1 ELSE 0 END)),0)AS cant_resto,
			COALESCE(COUNT(pre.id),0) AS cant_premios,
			-- CASE WHEN (s.sor_fechor_presc_recibida IS NULL  OR  s.sor_fechor_presc_recibida ='') THEN 1 ELSE 0 END AS presc_recibida_boldt,
			CASE WHEN (s.sor_fechor_presc_recibida IS NULL  OR  s.sor_fechor_presc_recibida ='')  AND idjuego NOT IN (50,41) THEN 1 ELSE 0 END AS presc_recibida_boldt,
			CASE WHEN s.fecha_prescripcion < CURDATE() THEN 1 ELSE 0 END  AS fecha_presc_menor_hoy,
			CASE WHEN DATEDIFF(DATE_ADD(CURDATE(), INTERVAL 45 DAY),s.fecha_prescripcion) >= p.dias_espera_cierre_emision THEN 1 ELSE 0 END AS dif_fec_presc_sup_dias_espera
				INTO @cant_conf_pend, @cant_resto, @cant_premios, @presc_recibida_boldt,@fecha_presc_menor_hoy,@dif_fec_presc_sup_dias_espera
			FROM sor_pgmsorteo s
				JOIN sor_producto p ON p.`id` = s.`sor_producto_id_c` AND p.deleted = 0
				LEFT JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			WHERE s.deleted = 0
			  AND s.id = @id_pgmsorteo
		;
		IF (@cant_conf_pend  > 0 ) THEN 
			UPDATE tmp_estado_proc_cierre_emis_presc 
			SET     estadoproc = 'ERR - Estado de Registro de Beneficiario CONFIRMADO y Estado de Pago PENDIENTE',
				cod_estado = '4',
				nro_sorteo = @sorteo,
				juego = @juego,
				date_modified = NOW()
			WHERE id_sorteo = @id_pgmsorteo 
			  AND idproceso = @id_consolidacion
			; 
			SET msgerr = '4-ERR'; -- no existe el id de sorteo
			-- actualizo a estado original para ser procesado luego siendo en ese momento cuando cumpla con las condiciones
			UPDATE sor_pgmsorteo 
			SET     sor_emision_cerrada = 0,
				sor_fechor_emision_cerrada = NULL 
			WHERE id = @id_pgmsorteo
			;
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - Validación no superada: ',msgerr));			
			LEAVE fin;
		END IF;
		
		IF (@presc_recibida_boldt = 1) THEN
			UPDATE tmp_estado_proc_cierre_emis_presc 
			SET     estadoproc = 'ERR - No se registra procesada la prescripción de BOLDT',
				cod_estado = '5',
				nro_sorteo = @sorteo,
				juego = @juego,
				date_modified = NOW()
			WHERE id_sorteo = @id_pgmsorteo 
			  AND idproceso = @id_consolidacion
			;
			SET msgerr = '5-ERR'; -- no existe el id de sorteo
			-- actualizo a estado original para ser procesado luego siendo en ese momento cuando cumpla con las condiciones
			UPDATE sor_pgmsorteo 
			SET     sor_emision_cerrada = 0,
				sor_fechor_emision_cerrada =  NULL 
			WHERE id = @id_pgmsorteo
			;
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - Validación no superada: ',msgerr));
			LEAVE fin;		
		END IF;
	
		-- validar fechas
		IF (@fecha_presc_menor_hoy = 0) THEN 
			UPDATE tmp_estado_proc_cierre_emis_presc 
			SET estadoproc = 'ERR - Fecha de prescripcion menor a fecha de proceso',
				cod_estado = '7',
				nro_sorteo = @sorteo,
				juego = @juego,
				date_modified = NOW()
			WHERE id_sorteo = @id_pgmsorteo 
				AND idproceso = @id_consolidacion
			;
			
			SET msgerr = '7-ERR'; -- no existe el id de sorteo
			-- actualizo a estado original para ser procesado luego siendo en ese momento cuando cumpla con las condiciones
			UPDATE sor_pgmsorteo 
			SET     sor_emision_cerrada = 0,
				sor_fechor_emision_cerrada = NULL 
			WHERE id = @id_pgmsorteo
			;
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - Validación no superada: ',msgerr));
			LEAVE fin;	
		END IF;
		
		IF (@dif_fec_presc_sup_dias_espera = 0 )  THEN 
			UPDATE tmp_estado_proc_cierre_emis_presc 
			SET     estadoproc = 'ERR - No se supera los días de espera de prescripcion', 
				cod_estado = '8', 
				nro_sorteo = @sorteo, 
				juego = @juego, 
				date_modified = NOW()
			WHERE id_sorteo = @id_pgmsorteo 
			  AND idproceso = @id_consolidacion
			;
			SET msgerr = '8-ERR'; -- se supera dias de espera
			-- actualizo a estado original para ser procesado luego siendo en ese momento cuando cumpla con las condiciones
			UPDATE sor_pgmsorteo 
			SET     sor_emision_cerrada = 0,
				sor_fechor_emision_cerrada =  NULL 
			WHERE id = @id_pgmsorteo
			;
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN ERR - Validación no superada: ',msgerr));
			LEAVE fin;	
		END IF;
		
		-- **************************************************   TODO OK - COMIENZA EL PROCESO *************************************************
		-- IF ((NOT (@juego = 50 OR @juego = 41)) AND (@emisionCerrada = 0)) THEN -- 0-> abierta   1->cerrada
		-- IF (@emisionCerrada = 0 ) THEN -- 0-> abierta   1->cerrada
		IF ((@emisionCerrada = 0 ) AND ((@juego = 29 AND @sorteo >1320) OR @juego != 29)) THEN -- 0-> abierta   1->cerrada
			-- Se rehace la tabla pre_resumen_emision como solución a que no de todos los puntos donde se actualizan estados de premios se actualiza el resumen,
			--   y resulta menos costoso en impacto y tiempo hacerlo de esta manera.
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - Recalcula Resúmenes de emisión (PREMIOS_CIERRE_recalcula_resumen_emision) - sorteo ->',@id_pgmsorteo, '<-'));
			CALL PREMIOS_CIERRE_recalcula_resumen_emision(@id_pgmsorteo,0, @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		END IF;
		
		/*
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - Insert tmp_tot: temporal de totales prescriptos '));
		-- inserto todo en la temporal
		DROP TEMPORARY TABLE IF EXISTS tmp_tot;
		CREATE TEMPORARY TABLE tmp_tot (
		  sor_pgmsorteo_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  tbl_provincias_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  pre_canal_de_pago_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
		  origen VARCHAR(20) CHARACTER SET utf8 NOT NULL,
		  pag_pre_bruto DECIMAL(18,2) DEFAULT NULL,
		  pag_pre_neto DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_total_sin_cudaio DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_20630 DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_23351 DECIMAL(18,2) DEFAULT NULL,
		  pag_ret_ley_11265 DECIMAL(18,2) DEFAULT NULL,
		  pag_cudaio DECIMAL(18,2) DEFAULT NULL,
		  PRIMARY KEY (sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,origen)
		) ENGINE=INNODB
		;
		-- Podria usar
		--				IF(pre.tbl_provincias_id_c = var_santafe, 'T', 'O')
		-- filtra PAGAAGENCIA = N!!!
		
		INSERT INTO tmp_tot
		SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
					ELSE 'NI' 
				END AS pre_canal_de_pago_id_c, 
				'pre' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.`id` = s.`sor_producto_id_c` AND p.deleted = 0
				INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
				WHERE s.id = @id_pgmsorteo AND s.deleted = 0 
					-- AND pre.pre_pagaagencia = 'N' -- No paga Agencia (Tesoreria / Otras Provincias)
					AND pre.pre_estadopago = 'E'  -- P(E)ndiente - P(R)escripto - P(A)gado
						
				GROUP BY s.id, pre.tbl_provincias_id_c, 
					CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
						-- WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
						-- WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
						WHEN (pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
						WHEN (pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
						ELSE 'NI' 
					END 
		;
		-- premios_menores
		INSERT INTO tmp_tot
		SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
					ELSE 'NI' END 		
				AS pre_canal_de_pago_id_c, 
				'premen' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.`id` = s.`sor_producto_id_c` AND p.deleted = 0
				INNER JOIN pre_premios_menores pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
				WHERE s.id = @id_pgmsorteo AND s.deleted = 0 
					-- AND pre.pre_pagaagencia = 'N' -- No paga Agencia (Tesoreria / Otras Provincias)
					AND pre.pre_estadopago = 'E'  -- P(E)ndiente - P(R)escripto - P(A)gado
				GROUP BY s.id, pre.tbl_provincias_id_c, 
					CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
					ELSE 'NI' END 
		;
		-- premios_menores
		INSERT INTO tmp_tot
		SELECT  	
				s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
					ELSE 'NI' END 		
				AS pre_canal_de_pago_id_c, 
				'premenH' AS origen,
				SUM(pre.pre_impbruto) AS pag_pre_bruto,
				SUM(pre.pre_impneto) AS pag_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS pag_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS pag_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS pag_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS pag_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS pag_cudaio
			FROM sor_pgmsorteo s
				INNER JOIN sor_producto p ON p.`id` = s.`sor_producto_id_c` AND p.deleted = 0
				INNER JOIN pre_premios_menores_historicos pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
				WHERE s.id = @id_pgmsorteo AND s.deleted = 0 
					-- AND pre.pre_pagaagencia = 'N' -- No paga Agencia (Tesoreria / Otras Provincias)
					AND pre.pre_estadopago = 'E'  -- P(E)ndiente - P(R)escripto - P(A)gado
				GROUP BY s.id, pre.tbl_provincias_id_c, 
					CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = var_santafe) THEN 'T' 
					WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != var_santafe) THEN 'O' 
					ELSE 'NI' END 	
		;
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - Fin Insert tmp_tot: temporal de totales prescriptos '));
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - Actualiza tabla Resumen Emision a partir de la tmp_tot '));
		-- actualizo los que existen
		UPDATE pre_resumen_emision r
			INNER JOIN (
			SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
				SUM(pre.pag_pre_bruto) AS pag_pre_bruto,
				SUM(pre.pag_pre_neto) AS pag_pre_neto,
				SUM(pre.pag_ret_total_sin_cudaio) AS pag_ret_total_sin_cudaio,
				SUM(pre.pag_ret_ley_20630) AS pag_ret_ley_20630,
				SUM(pre.pag_ret_ley_23351) AS pag_ret_ley_23351,
				SUM(pre.pag_ret_ley_11265) AS pag_ret_ley_11265,
				SUM(pre.pag_cudaio) AS pag_cudaio
			FROM tmp_tot pre
			GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
			) t ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
		SET r.date_modified = NOW(),
		    r.aprs_pre_bruto = 0,
		    r.aprs_pre_neto = 0,
		    r.aprs_ret_total_sin_cudaio = 0,
		    r.aprs_ret_ley_20630 = 0,
		    r.aprs_ret_ley_23351 = 0,
		    r.aprs_ret_ley_11265 = 0,
		    r.aprs_cudaio = 0,    
		    
		    r.prs_pre_bruto = t.pag_pre_bruto,
		    r.prs_pre_neto = t.pag_pre_neto,
		    r.prs_ret_total_sin_cudaio = t.pag_ret_total_sin_cudaio,
		    r.prs_ret_ley_20630 = t.pag_ret_ley_20630,
		    r.prs_ret_ley_23351 = t.pag_ret_ley_23351,
		    r.prs_ret_ley_11265 = t.pag_ret_ley_11265,
		    r.prs_cudaio = t.pag_cudaio                   
		;
		-- inserto los que no existen
		INSERT INTO pre_resumen_emision (id,NAME,date_entered,date_modified,modified_user_id,created_by,description,deleted,assigned_user_id,
							sor_pgmsorteo_id_c,tbl_provincias_id_c,pre_canal_de_pago_id_c,
							liq_pre_bruto, liq_pre_neto, liq_ret_total_sin_cudaio, liq_ret_ley_20630, liq_ret_ley_23351, liq_ret_ley_11265, liq_cudaio,
							pag_pre_bruto, pag_pre_neto, pag_ret_total_sin_cudaio, pag_ret_ley_20630, pag_ret_ley_23351, pag_ret_ley_11265, pag_cudaio,
							aprs_pre_bruto, aprs_pre_neto, aprs_ret_total_sin_cudaio, aprs_ret_ley_20630, aprs_ret_ley_23351, aprs_ret_ley_11265, aprs_cudaio,
							prs_pre_bruto, prs_pre_neto, prs_ret_total_sin_cudaio, prs_ret_ley_20630, prs_ret_ley_23351, prs_ret_ley_11265, prs_cudaio,
							prs_pre_bruto_mens, prs_pre_neto_mens, prs_ret_total_mens, prs_ret_ley20630_mens, prs_ret_ley23351_mens, procesado, estado)
		SELECT
			UUID() AS id,
			CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', pro.name) AS `name`,
			NOW() AS date_entered,
			NOW() AS date_modified,
			'1' AS modified_user_id,
			'1' AS created_by,
			CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',t.pre_canal_de_pago_id_c,'_', pro.name) AS description,
			0 AS deleted,
			'1' AS assigned_user_id,
			t.sor_pgmsorteo_id_c, 
			t.tbl_provincias_id_c AS tbl_provincias_id_c,
			t.pre_canal_de_pago_id_c, 
			
			0 AS liq_pre_bruto,
			0 AS liq_pre_neto,
			0 AS liq_ret_total_sin_cudaio,
			0 AS liq_ret_ley_20630,
			0 AS liq_ret_ley_23351,
			0 AS liq_ret_ley_11265,
			0 AS liq_cudaio,
			
			0 AS pag_pre_bruto,
			0 AS pag_pre_neto,
			0 AS pag_ret_total_sin_cudaio,
			0 AS pag_ret_ley_20630,
			0 AS pag_ret_ley_23351,
			0 AS pag_ret_ley_11265,
			0 AS pag_cudaio,
			
			0 AS aprs_pre_bruto,
			0 AS aprs_pre_neto,
			0 AS aprs_ret_total_sin_cudaio,
			0 AS aprs_ret_ley_20630,
			0 AS aprs_ret_ley_23351,
			0 AS aprs_ret_ley_11265,
			0 AS aprs_cudaio,
			
			t.pag_pre_bruto AS prs_pre_bruto,
			t.pag_pre_neto AS prs_pre_neto,
			t.pag_ret_total_sin_cudaio AS prs_ret_total_sin_cudaio,
			t.pag_ret_ley_20630 AS prs_ret_ley_20630,
			t.pag_ret_ley_23351 AS prs_ret_ley_23351,
			t.pag_ret_ley_11265 AS prs_ret_ley_11265,
			t.pag_cudaio AS prs_cudaio, 
			
			0 AS prs_pre_bruto_mens,
			0 AS prs_pre_neto_mens,
			0 AS prs_ret_total_mens,
			0 AS prs_ret_ley_20630_mens,
			0 AS prs_ret_ley_23351_mens,
			0 as procesado,
			'1' as estado
		FROM  
			(
			SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
					SUM(pre.pag_pre_bruto) AS pag_pre_bruto,
					SUM(pre.pag_pre_neto) AS pag_pre_neto,
					SUM(pre.pag_ret_total_sin_cudaio) AS pag_ret_total_sin_cudaio,
					SUM(pre.pag_ret_ley_20630) AS pag_ret_ley_20630,
					SUM(pre.pag_ret_ley_23351) AS pag_ret_ley_23351,
					SUM(pre.pag_ret_ley_11265) AS pag_ret_ley_11265,
					SUM(pre.pag_cudaio) AS pag_cudaio
				FROM tmp_tot pre
				GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
			) t
				INNER JOIN sor_pgmsorteo s ON s.id = t.sor_pgmsorteo_id_c AND s.deleted=0
				INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
				INNER JOIN tbl_provincias pro ON pro.id = t.tbl_provincias_id_c AND pro.deleted = 0
				LEFT JOIN  pre_resumen_emision r ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
			WHERE r.sor_pgmsorteo_id_c IS NULL
		;
		*/
		
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN Actualiza tabla Resumen Emision: campos aprs -> campos prs'));		
		START TRANSACTION;
			UPDATE pre_resumen_emision re			
				SET 	
					re.prs_pre_bruto =            CASE WHEN COALESCE(re.aprs_pre_bruto,0)>0            THEN COALESCE(re.aprs_pre_bruto,0)            ELSE re.prs_pre_bruto END,
					re.prs_pre_neto =             CASE WHEN COALESCE(re.aprs_pre_neto,0)>0             THEN COALESCE(re.aprs_pre_neto,0)             ELSE re.prs_pre_neto END,
					re.prs_ret_total_sin_cudaio = CASE WHEN COALESCE(re.aprs_ret_total_sin_cudaio,0)>0 THEN COALESCE(re.aprs_ret_total_sin_cudaio,0) ELSE re.prs_ret_total_sin_cudaio END,
					re.prs_ret_ley_20630        = CASE WHEN COALESCE(re.aprs_ret_ley_20630,0)>0        THEN COALESCE(re.aprs_ret_ley_20630,0)        ELSE re.prs_ret_ley_20630 END,
					re.prs_ret_ley_23351        = CASE WHEN COALESCE(re.aprs_ret_ley_23351,0)>0        THEN COALESCE(re.aprs_ret_ley_23351,0)        ELSE re.prs_ret_ley_23351 END,
					re.prs_ret_ley_11265        = CASE WHEN COALESCE(re.aprs_ret_ley_11265,0)>0        THEN COALESCE(re.aprs_ret_ley_11265,0)        ELSE re.prs_ret_ley_11265 END,
					re.prs_cudaio               = CASE WHEN COALESCE(re.aprs_cudaio,0)>0               THEN COALESCE(re.aprs_cudaio,0)               ELSE re.prs_cudaio END,
					re.estado = '2',
					re.procesado = 1,
					re.date_modified = @hoy
			WHERE re.sor_pgmsorteo_id_c = @id_pgmsorteo
			;
			UPDATE pre_resumen_emision re			
				SET 	
					re.aprs_pre_bruto = 0,
					re.aprs_pre_neto = 0,
					re.aprs_ret_total_sin_cudaio = 0,
					re.aprs_ret_ley_20630 = 0,
					re.aprs_ret_ley_23351 = 0,
					re.aprs_ret_ley_11265 = 0,
					re.aprs_cudaio = 0,
					re.estado = '2',
					re.procesado = 1,
					re.date_modified = @hoy
			WHERE re.sor_pgmsorteo_id_c = @id_pgmsorteo
			;
		COMMIT;
				
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN Actualiza tabla Resumen Emision: campos aprs -> campos prs'));

		-- actualizo a estado de premio a "PRESCRIPTO"	
		SET @idenprc = CONCAT('ppre_', @id_consolidacion,'_',LPAD(@juego,3,0),LPAD(@sorteo,6,0));

		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - INI Actualiza padron de premios a estadoPago = R = Prescripto ')); 			
		START TRANSACTION;
			UPDATE pre_premios prem 
					SET prem.pre_estadopago = 'R',
						prem.pre_prcpagpres = @idenprc,
						prem.date_modified  = @hoy
			WHERE prem.sor_pgmsorteo_id_c = @id_pgmsorteo
				AND prem.pre_estadopago = 'E';
			UPDATE pre_premios_menores prem 
					SET prem.pre_estadopago = 'R',
						prem.pre_prcpagpres = @idenprc,
						prem.date_modified  = @hoy
			WHERE prem.sor_pgmsorteo_id_c = @id_pgmsorteo
				AND prem.pre_estadopago = 'E';
		COMMIT;
		
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN OK Actualiza padron de premios a estadoPago = R = Prescripto '));
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - INI Actualiza ESTADO emisión y resumen_emision '));
		-- START TRANSACTION;
			-- MARCAR LA EMISION COMO CERRADA (sor_emision_cerrada=1). Para los juegos Loteria y Doble Ch, debido a que no hay prescripción de boldt, la marca como recibida para que quede consistente.
			UPDATE sor_pgmsorteo 
				SET 	sor_emision_cerrada=1, 
					sor_fechor_emision_cerrada = @hoy,
					sor_fechor_presc_recibida = IF(idjuego IN (50, 41), @hoy, sor_fechor_presc_recibida),
					sor_presc_recibida = IF(idjuego IN (50, 41), 1, sor_presc_recibida)
			WHERE id = @id_pgmsorteo;
			-- MARCAR LOS RESUMENES DE EMISION COMO PREMIOS CERRADOS (estado=2)
			UPDATE pre_resumen_emision re			
			SET 	re.estado = '2',
				re.procesado = 1,
				re.date_modified = @hoy
			WHERE re.sor_pgmsorteo_id_c = @id_pgmsorteo;
			IF (!(@juego = 4 OR @juego = 13 OR @juego = 30)) THEN
				INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - ini proceso sor_publica_minutas - proceso:', @idenprc);
						
				CALL sor_publica_minutas(@juego, @sorteo, '1', @idenprc, @rcode, @rtxt, @id, @rsqlerrno, @rsqlerrtxt);
				INSERT INTO kk_auditoria2(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - fin proceso sor_publica_minutas - juego:', @juego, ' - sorteo:', @sorteo, ' - proceso:', @idenprc, ' - resultado ', @rcode, '-', @rtxt);
				
				IF (@rcode != 0) THEN
					ROLLBACK;
					SET msgerr=CONCAT("Error durante contabilizacion del sorteo - error:", @rcode, " mensaje:", @rtxt);
					SELECT msgerr;
					LEAVE fin;
				END IF;
			END IF;
		-- COMMIT;
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN OK Actualiza ESTADO emisión y resumen_emision '));

		/* Esto pasa a otra opcion del Menu de Suite que lo ejecuta E Adh.!!
		-- para Q6, Brinco y Poceada Federal, genera NC en manuales por la prescripcion
		IF (@juego = 4 OR @juego = 13 OR @juego = 30) THEN
			CALL CC_NC_Prescripcion_CtaCte_Provincia(@id_pgmsorteo, '1', @rcode, @rtxt, @id, @rsqlerrno, @rsqlerrtxt);
			IF (@rcode != 1) THEN
				SET msgerr=CONCAT("Error durante generacion NC prescriptos - provincia - error:", @rcode, " mensaje:", @rtxt);
				SELECT msgerr;
				LEAVE fin;
			END IF;
		END IF;*/

		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - Registra en tmp_estado_proc_cierre_emis_presc que el cierre de premios termino OK '));
		UPDATE tmp_estado_proc_cierre_emis_presc 
			SET     estadoproc = 'OK' , 
				cod_estado = '0', 
				nro_sorteo = @sorteo, 
				juego = @juego 
			WHERE id_sorteo = @id_pgmsorteo 
			  AND idproceso = @id_consolidacion
		;
		IF NOT EXISTS (SELECT * FROM cas02_cc_cntpoc_cab 
				WHERE cab_nrominuta = 92 
				  AND sor_pgmsorteo_id_c = (
					SELECT id FROM sor_pgmsorteo 
					WHERE idjuego = @juego 
					  AND nrosorteo = @sorteo)
			      ) THEN
			      IF (!(@juego = 4 OR @juego = 13)) THEN
					UPDATE sor_pgmsorteo
					SET sor_presc_contabilizada = 1,
					    sor_fechor_presc_contabilizada = NOW()
					WHERE idjuego = @juego AND nrosorteo = @sorteo
					;
					UPDATE pre_resumen_emision re			
					SET 	re.estado = '6',
						re.procesado = 1,
						re.date_modified = @hoy
					WHERE re.sor_pgmsorteo_id_c = @id_pgmsorteo;
				END IF;
		END IF;
		
		SET msgerr = 'OK'; 
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision - FIN OK - ID SORTEO: ', id_pgmsorteo, ' - ORDEN PAGO: ', id_ordenpago, ' - id_consolidacion: ', id_consolidacion, ' - opc: ', opc));
		
		LEAVE fin;
	END IF;
    END$$

DELIMITER ;