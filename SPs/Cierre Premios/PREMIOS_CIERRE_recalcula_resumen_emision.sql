DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_CIERRE_recalcula_resumen_emision`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_CIERRE_recalcula_resumen_emision`(
		IN id_pgmsorteo CHAR(36),
		IN id_consolidacion INT,
		OUT RCode INT,
		OUT RTxt VARCHAR (500),
		OUT RId VARCHAR (36),
		OUT RSQLErrNo INT,
		OUT RSQLErrtxt VARCHAR (500)
)
thisSP :
BEGIN
/*	
-- opcion 1 -> liquidacion de premios
-- opcion 2 -> pago de premios
-- opcion 3 -> prescripcion premios agencias
-- opcion 4 -> prescripcion premios por cierre de emision (OTRAS PROVINCIAS + TESORERIA)
*/	
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
	SET RSQLErrNo = 1;
	SET RSQLErrtxt = CONCAT("PREMIOS_CIERRE_recalcula_resumen_emision ", @msg);
		
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_CIERRE_recalcula_resumen_emision - FIN CON ERROR - code:', @code, ' - errno:', @errno, ' - msg:', @msg));
	END
	;
	
	SET RCode = 1 ;
	SET RTxt = 'OK' ;
	SET RSQLErrNo = 0 ;
	SET RSQLErrtxt = "OK" ;
	SET RId = 0 ;	
	
	SET @id_pgmsorteo = id_pgmsorteo
	;
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - INI ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
	
	IF EXISTS(SELECT * FROM sor_pgmsorteo WHERE id = @id_pgmsorteo AND sor_emision_cerrada = 1) THEN
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - FIN: Emisión ya cerrada. No se actualiza el resumen. ID SORTEO: ', id_pgmsorteo, ' - ID PRO CONSOLIDACION: id_consolidacion ', id_consolidacion));
		
		SET RCode = 0 ;
		SET RTxt = 'Emisión ya cerrada. No se actualiza el resumen.' ;
		SET RSQLErrNo = 0 ;
		SET RSQLErrtxt = "OK" ;
		SET RId = id_pgmsorteo ;	
	END IF;

	DROP TEMPORARY TABLE IF EXISTS tmp_tot;
	CREATE TEMPORARY TABLE tmp_tot (
	  sor_pgmsorteo_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
	  tbl_provincias_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
	  pre_canal_de_pago_id_c CHAR(36) CHARACTER SET utf8 NOT NULL,
	  origen VARCHAR(12) CHARACTER SET utf8 NOT NULL,
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
	-- --------------------------------------------------------------------------------
	-- CALCULO resumen_emision LIQUIDADOS
	-- --------------------------------------------------------------------------------
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - LIQUIDADOS ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
	-- premios 
	INSERT INTO tmp_tot
		SELECT  s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
				WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
				WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
				ELSE 'NI' 
			END AS canal, 
			'pre_liq' AS origen,
			SUM(pre.pre_impbruto) AS liq_pre_bruto,
			SUM(pre.pre_impneto) AS liq_pre_neto,
			SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS liq_ret_total_sin_cudaio,
			SUM(pre.pre_ret_ley20630) AS liq_ret_ley_20630,
			SUM(pre.pre_ret_ley23351) AS liq_ret_ley_23351,
			SUM(pre.pre_ret_ley11265) AS liq_ret_ley_11265,
			SUM(pre.pre_ret_ley11265) AS liq_cudaio
		FROM sor_pgmsorteo s
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		/*AND (
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		)
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
		AND s.id = @id_pgmsorteo
		GROUP BY s.id, pre.tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
				ELSE 'NI' 
			END
	;
	-- premios menores
	INSERT INTO tmp_tot
		SELECT  	s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'premen_liq' AS origen,
				SUM(pre.pre_impbruto) AS liq_pre_bruto,
				SUM(pre.pre_impneto) AS liq_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS liq_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS liq_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS liq_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS liq_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS liq_cudaio
		FROM sor_pgmsorteo s
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN pre_premios_menores pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		/*AND (
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		)
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
		AND s.id = @id_pgmsorteo
		GROUP BY s.id, pre.tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
				ELSE 'NI' 
			END
	;
	
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	-- ESTO TIENE QUE DESAPARECER, YA NO HAY NADA QUE INCORPORAR DESDE LOS HISTORICOS!!!!
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	
	-- premios menores historicos
	INSERT INTO tmp_tot
		SELECT  	s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'premenH_liq' AS origen,
				SUM(pre.pre_impbruto) AS liq_pre_bruto,
				SUM(pre.pre_impneto) AS liq_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS liq_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS liq_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS liq_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS liq_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS liq_cudaio
		FROM sor_pgmsorteo s
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN pre_premios_menores_historicos pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		/*AND (
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		)
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
		AND s.id = @id_pgmsorteo
		GROUP BY s.id, pre.tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
				ELSE 'NI' 
			END
	;
	-- --------------------------------------------------------------------------------
	-- CALCULO resumen_emision PAGADOS
	-- --------------------------------------------------------------------------------
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - PAGADOS ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	-- ASUME LOS PAGADOS aunque NO HAYAN SIDO INFORMADOS!!! 
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	
	-- premios mayores
	INSERT INTO tmp_tot
	SELECT  	
			s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			COALESCE(op.pre_canal_de_pago_id_c,'A') AS pre_canal_de_pago_id_c, -- si el premio esta pagado y la op no tiene el canal de pago es porque no esta registrado el beneficiario, ergo es un premio pagado en agencia para el cual el archivo de pagos se proceso antes del beneficiario
			'pre_pag' AS origen,
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
	WHERE s.deleted = 0 
	/*AND (
		(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
		OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
	)
	AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
	AND s.id = @id_pgmsorteo
	AND pre.pre_estadopago = 'A' 
	GROUP BY s.id, pre.tbl_provincias_id_c, COALESCE(op.pre_canal_de_pago_id_c,'A')
	;
	-- premios menores
	INSERT INTO tmp_tot
	SELECT  	
			s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
			ELSE 'NI' END 		
			AS pre_canal_de_pago_id_c, 
			'premen_pag' AS origen,
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
	WHERE s.deleted = 0 
	/*AND (
		(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
		OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
	)
	AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
	AND s.id = @id_pgmsorteo
	AND pre.pre_estadopago = 'A' 
	GROUP BY s.id, pre.tbl_provincias_id_c, 
		CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
			ELSE 'NI' 
		END
	;
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	-- ESTO TIENE QUE DESAPARECER, YA NO HAY NADA QUE INCORPORAR DESDE LOS HISTORICOS!!!!
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	-- premios menores historicos
	INSERT INTO tmp_tot
	SELECT  	
			s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
			ELSE 'NI' END 		
			AS pre_canal_de_pago_id_c, 
			'premenH_pag' AS origen,
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
	WHERE s.deleted = 0 
	/*AND (
		(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
		OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
	)
	AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
	AND s.id = @id_pgmsorteo
	AND pre.pre_estadopago = 'A' 
	GROUP BY s.id, pre.tbl_provincias_id_c, 
		CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
			WHEN (pre.pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
			ELSE 'NI' 
		END
	;
	-- --------------------------------------------------------------------------------
	-- CALCULO resumen_emision PRESCRIPTOS AGENCIAS
	-- --------------------------------------------------------------------------------
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - PRESCRIPTOS AGENCIAS ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
	
	/* lo calcula sobre la pre_premios considerando el estado de pago 
	
	INSERT INTO tmp_tot
		SELECT  pgm.id AS sor_pgmsorteo_id_c, 
			'S' AS tbl_provincias_id_c,
			'A' AS pre_canal_de_pago_id_c,     -- SOLO SE RECIBEN PRESCRIPCIONES DE TIPO AGENCIA!
			'afect_prs' AS origen,             -- UN ÚNICO ORIGEN!!!
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
			INNER JOIN sor_pgmsorteo pgm ON pgm.sor_producto_id_c = cab.cas02_cc_juegos_id_c AND pgm.nrosorteo = cab.ac_sorteo
		WHERE cab.deleted = 0
		AND pgm.id = @id_pgmsorteo
		GROUP BY pgm.id
	;
	*/
	
	-- 8-10-2024 - lmariotti	
	-- premios mayores prescriptos en boldt
	
	INSERT INTO tmp_tot
		SELECT  	
			s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			'A' AS pre_canal_de_pago_id_c, 
			'mayores_prs'  AS origen,
			SUM(pre.pre_impbruto) AS prs_pre_bruto,
			SUM(pre.pre_impneto) AS prs_pre_neto,
			SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS prs_ret_total_sin_cudaio,
			SUM(pre.pre_ret_ley20630) AS prs_ret_ley_20630,
			SUM(pre.pre_ret_ley23351) AS prs_ret_ley_23351,
			SUM(pre.pre_ret_ley11265) AS prs_ret_ley_11265,
			SUM(pre.pre_ret_ley11265) AS prs_cudaio
		FROM sor_pgmsorteo s
		INNER JOIN sor_producto p ON p.id_juego  = s.idjuego AND p.deleted = 0
		INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
		INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		AND s.id = @id_pgmsorteo
		AND pre.pre_estadopago = 'R'  AND pre.tbl_provincias_id_c = 'S'
		GROUP BY s.id, pre.tbl_provincias_id_c;
	
	-- premios menores prescriptos en boldt
	
	INSERT INTO tmp_tot
		SELECT  	
			s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			'A' AS pre_canal_de_pago_id_c, 
			'menores_prs'  AS origen,
			SUM(pre.pre_impbruto) AS prs_pre_bruto,
			SUM(pre.pre_impneto) AS prs_pre_neto,
			SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS prs_ret_total_sin_cudaio,
			SUM(pre.pre_ret_ley20630) AS prs_ret_ley_20630,
			SUM(pre.pre_ret_ley23351) AS prs_ret_ley_23351,
			SUM(pre.pre_ret_ley11265) AS prs_ret_ley_11265,
			SUM(pre.pre_ret_ley11265) AS prs_cudaio
		FROM sor_pgmsorteo s
		INNER JOIN sor_producto p ON p.id_juego  = s.idjuego AND p.deleted = 0
		INNER JOIN pre_premios_menores pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
		INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		AND s.id = @id_pgmsorteo
		AND pre.pre_estadopago = 'R' AND pre.tbl_provincias_id_c = 'S'
		GROUP BY s.id, pre.tbl_provincias_id_c;
	

	-- --------------------------------------------------------------------------------
	-- CALCULO resumen_emision A PRESCRIBIR
	-- --------------------------------------------------------------------------------
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - A PRESCRIBIR ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
	
	-- premios 
	INSERT INTO tmp_tot
		SELECT  s.id AS sor_pgmsorteo_id_c, 
			pre.tbl_provincias_id_c AS tbl_provincias_id_c,
			CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
				WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
				WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
				ELSE 'NI' 
			END AS canal, 
			'pre_aprs' AS origen,
			SUM(pre.pre_impbruto) AS liq_pre_bruto,
			SUM(pre.pre_impneto) AS liq_pre_neto,
			SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS liq_ret_total_sin_cudaio,
			SUM(pre.pre_ret_ley20630) AS liq_ret_ley_20630,
			SUM(pre.pre_ret_ley23351) AS liq_ret_ley_23351,
			SUM(pre.pre_ret_ley11265) AS liq_ret_ley_11265,
			SUM(pre.pre_ret_ley11265) AS liq_cudaio
		FROM sor_pgmsorteo s
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN pre_premios pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		/*AND (
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		)
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
		AND s.id = @id_pgmsorteo
		AND pre.pre_estadopago = 'E'
		GROUP BY s.id, pre.tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
				ELSE 'NI' 
			END
	;
	-- premios menores
	INSERT INTO tmp_tot
		SELECT  	s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'premen_aprs' AS origen,
				SUM(pre.pre_impbruto) AS liq_pre_bruto,
				SUM(pre.pre_impneto) AS liq_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS liq_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS liq_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS liq_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS liq_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS liq_cudaio
		FROM sor_pgmsorteo s
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN pre_premios_menores pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		/*AND (
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		)
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
		AND s.id = @id_pgmsorteo
		AND pre.pre_estadopago = 'E'
		GROUP BY s.id, pre.tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
				ELSE 'NI' 
			END
	;
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	-- ESTO TIENE QUE DESAPARECER, YA NO HAY NADA QUE INCORPORAR DESDE LOS HISTORICOS!!!!
	-- ----------------------------------- ADEN - 2024-04-22 ------------------------------------------------------------------------------------
	-- premios menores historicos
	INSERT INTO tmp_tot
		SELECT  	s.id AS sor_pgmsorteo_id_c, 
				pre.tbl_provincias_id_c AS tbl_provincias_id_c,
				CASE 	WHEN pre_pagaagencia = 'S' THEN  'A'
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c = 'S') THEN 'T' 
					WHEN (pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S') THEN 'O' 
					ELSE 'NI' 
				END AS canal, 
				'premenH_aprs' AS origen,
				SUM(pre.pre_impbruto) AS liq_pre_bruto,
				SUM(pre.pre_impneto) AS liq_pre_neto,
				SUM(pre.pre_ret_ley20630+pre.pre_ret_ley23351) AS liq_ret_total_sin_cudaio,
				SUM(pre.pre_ret_ley20630) AS liq_ret_ley_20630,
				SUM(pre.pre_ret_ley23351) AS liq_ret_ley_23351,
				SUM(pre.pre_ret_ley11265) AS liq_ret_ley_11265,
				SUM(pre.pre_ret_ley11265) AS liq_cudaio
		FROM sor_pgmsorteo s
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN pre_premios_menores_historicos pre ON pre.sor_pgmsorteo_id_c = s.id AND pre.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = pre.tbl_provincias_id_c AND pro.deleted = 0
		WHERE s.deleted = 0 
		 /*(
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		      )
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL */
		AND s.id = @id_pgmsorteo
		AND pre.pre_estadopago = 'E'
		GROUP BY s.id, pre.tbl_provincias_id_c,
			CASE 	WHEN pre.pre_pagaagencia = 'S' THEN  'A'
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c  = 'S' THEN 'T' 
				WHEN pre_pagaagencia = 'N' AND pre.tbl_provincias_id_c != 'S' THEN 'O' 
				ELSE 'NI' 
			END
	;
	-- ------------------------------------------------------------------------------------------------------------
	-- CALCULO E INSERTO EN resumen_emision
	-- ------------------------------------------------------------------------------------------------------------
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - CALCULO RESUMEN ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
	/*
	DROP TEMPORARY TABLE IF EXISTS tmp_resumen_emision;
	CREATE TEMPORARY TABLE tmp_resumen_emision
		SELECT		UUID() AS id,
				CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',tt.pre_canal_de_pago_id_c,'_', CASE WHEN tt.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS NAME,
				NOW() AS date_entered,
				NOW() AS date_modified,
				'1' AS modified_user_id,
				'1' AS created_by,
				CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',tt.pre_canal_de_pago_id_c,'_', CASE WHEN tt.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS description,
				0 AS deleted,
				'1' AS assigned_user_id,
				tt.sor_pgmsorteo_id_c, 
				tt.tbl_provincias_id_c AS tbl_provincias_id_c,
				tt.pre_canal_de_pago_id_c,
				
				tt.liq_pre_bruto,
				tt.liq_pre_neto,
				tt.liq_ret_total_sin_cudaio,
				tt.liq_ret_ley_20630,
				tt.liq_ret_ley_23351,
				tt.liq_ret_ley_11265,
				tt.liq_cudaio,
				
				tt.pag_pre_bruto,
				tt.pag_pre_neto,
				tt.pag_ret_total_sin_cudaio,
				tt.pag_ret_ley_20630,
				tt.pag_ret_ley_23351,
				tt.pag_ret_ley_11265,
				tt.pag_cudaio,
			
				tt.prs_pre_bruto,
				tt.prs_pre_neto,
				tt.prs_ret_total_sin_cudaio,
				tt.prs_ret_ley_20630,
				tt.prs_ret_ley_23351,
				tt.prs_ret_ley_11265,
				tt.prs_cudaio, 
				
				0 AS prs_pre_bruto_mens,
				0 AS prs_pre_neto_mens,
				0 AS prs_ret_total_mens,
				0 AS prs_ret_ley_20630_mens,
				0 AS prs_ret_ley_23351_mens,
				
				tt.aprs_pre_bruto,
				tt.aprs_pre_neto,
				tt.aprs_ret_total_sin_cudaio,
				tt.aprs_ret_ley_20630,
				tt.aprs_ret_ley_23351,
				tt.aprs_ret_ley_11265,
				tt.aprs_cudaio
			FROM  
			(
			SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_pre_bruto ELSE 0 END) AS liq_pre_bruto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_pre_neto ELSE 0 END) AS liq_pre_neto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS liq_ret_total_sin_cudaio,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_ley_20630 ELSE 0 END) AS liq_ret_ley_20630,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_ley_23351 ELSE 0 END) AS liq_ret_ley_23351,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_ley_11265 ELSE 0 END) AS liq_ret_ley_11265,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_cudaio ELSE 0 END) AS liq_cudaio,
				
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_pre_bruto ELSE 0 END) AS pag_pre_bruto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_pre_neto ELSE 0 END) AS pag_pre_neto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS pag_ret_total_sin_cudaio,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_ley_20630 ELSE 0 END) AS pag_ret_ley_20630,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_ley_23351 ELSE 0 END) AS pag_ret_ley_23351,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_ley_11265 ELSE 0 END) AS pag_ret_ley_11265,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_cudaio ELSE 0 END) AS pag_cudaio, 
				
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_pre_bruto ELSE 0 END) AS prs_pre_bruto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_pre_neto ELSE 0 END) AS prs_pre_neto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS prs_ret_total_sin_cudaio,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_ley_20630 ELSE 0 END) AS prs_ret_ley_20630,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_ley_23351 ELSE 0 END) AS prs_ret_ley_23351,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_ley_11265 ELSE 0 END) AS prs_ret_ley_11265,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_cudaio ELSE 0 END) AS prs_cudaio,
				
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_pre_bruto ELSE 0 END) AS aprs_pre_bruto,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_pre_neto ELSE 0 END) AS aprs_pre_neto,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS aprs_ret_total_sin_cudaio,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_ley_20630 ELSE 0 END) AS aprs_ret_ley_20630,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_ley_23351 ELSE 0 END) AS aprs_ret_ley_23351,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_ley_11265 ELSE 0 END) AS aprs_ret_ley_11265,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_cudaio ELSE 0 END) AS aprs_cudaio
			FROM tmp_tot t
			GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
			) tt
			INNER JOIN sor_pgmsorteo s ON s.id = tt.sor_pgmsorteo_id_c AND s.deleted=0
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = tt.tbl_provincias_id_c AND pro.deleted = 0
			-- LEFT JOIN  pre_resumen_emision r ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
			-- WHERE r.sor_pgmsorteo_id_c IS NULL
			
	; */
	-- ---------------------------------------------------------------------------------------------------------------
	DELETE re 
		FROM pre_resumen_emision re
			INNER JOIN sor_pgmsorteo s ON s.id = re.sor_pgmsorteo_id_c AND s.deleted=0
	WHERE re.sor_pgmsorteo_id_c = @id_pgmsorteo 
		AND re.deleted = 0 
		AND (COALESCE(s.sor_presc_recibida, 0) = (CASE WHEN s.idjuego IN (4,5,13,29) THEN 0 ELSE COALESCE(s.sor_presc_recibida, 0) END)
		     OR (re.pre_canal_de_pago_id_c = 'O')
		     )
		 /*(
			(s.idjuego = 4 AND s.nrosorteo BETWEEN 3096 AND 3099)
			OR (s.idjuego = 13 AND s.nrosorteo BETWEEN 1215 AND 1216)
		      )
		AND s.sor_presc_recibida = 1 AND s.sor_fechor_presc_recibida IS NOT NULL 
		*/
	;
	INSERT INTO pre_resumen_emision 
		SELECT		UUID() AS id,
				CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',tt.pre_canal_de_pago_id_c,'_', CASE WHEN tt.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS NAME,
				NOW() AS date_entered,
				NOW() AS date_modified,
				'1' AS modified_user_id,
				'1' AS created_by,
				CONCAT('Resumen Sorteo: ',s.idjuego,'_',p.name,'_',s.nrosorteo,'_',tt.pre_canal_de_pago_id_c,'_', CASE WHEN tt.pre_canal_de_pago_id_c ='A' THEN 'AGENCIAS' ELSE UPPER(TRIM(pro.name)) END) AS description,
				0 AS deleted,
				'1' AS assigned_user_id,
				tt.sor_pgmsorteo_id_c, 
				tt.tbl_provincias_id_c AS tbl_provincias_id_c,
				tt.pre_canal_de_pago_id_c,
				
				tt.liq_pre_bruto,
				tt.liq_pre_neto,
				tt.liq_ret_total_sin_cudaio,
				tt.liq_ret_ley_20630,
				tt.liq_ret_ley_23351,
				tt.liq_ret_ley_11265,
				tt.liq_cudaio,
				
				tt.pag_pre_bruto,
				tt.pag_pre_neto,
				tt.pag_ret_total_sin_cudaio,
				tt.pag_ret_ley_20630,
				tt.pag_ret_ley_23351,
				tt.pag_ret_ley_11265,
				tt.pag_cudaio,
			
				tt.prs_pre_bruto,
				tt.prs_pre_neto,
				tt.prs_ret_total_sin_cudaio,
				tt.prs_ret_ley_20630,
				tt.prs_ret_ley_23351,
				tt.prs_ret_ley_11265,
				tt.prs_cudaio, 
				
				0 AS prs_pre_bruto_mens,
				0 AS prs_pre_neto_mens,
				0 AS prs_ret_total_mens,
				0 AS prs_ret_ley_20630_mens,
				0 AS prs_ret_ley_23351_mens,
				
				tt.aprs_pre_bruto,
				tt.aprs_pre_neto,
				tt.aprs_ret_total_sin_cudaio,
				tt.aprs_ret_ley_20630,
				tt.aprs_ret_ley_23351,
				tt.aprs_ret_ley_11265,
				tt.aprs_cudaio,
				0 AS procesado,
				'1' AS estado
			FROM  
			(
			SELECT sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_pre_bruto ELSE 0 END) AS liq_pre_bruto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_pre_neto ELSE 0 END) AS liq_pre_neto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS liq_ret_total_sin_cudaio,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_ley_20630 ELSE 0 END) AS liq_ret_ley_20630,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_ley_23351 ELSE 0 END) AS liq_ret_ley_23351,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_ret_ley_11265 ELSE 0 END) AS liq_ret_ley_11265,
				SUM(CASE RIGHT(t.origen,4) WHEN '_liq' THEN t.pag_cudaio ELSE 0 END) AS liq_cudaio,
				
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_pre_bruto ELSE 0 END) AS pag_pre_bruto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_pre_neto ELSE 0 END) AS pag_pre_neto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS pag_ret_total_sin_cudaio,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_ley_20630 ELSE 0 END) AS pag_ret_ley_20630,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_ley_23351 ELSE 0 END) AS pag_ret_ley_23351,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_ret_ley_11265 ELSE 0 END) AS pag_ret_ley_11265,
				SUM(CASE RIGHT(t.origen,4) WHEN '_pag' THEN t.pag_cudaio ELSE 0 END) AS pag_cudaio, 
				
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_pre_bruto ELSE 0 END) AS prs_pre_bruto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_pre_neto ELSE 0 END) AS prs_pre_neto,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS prs_ret_total_sin_cudaio,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_ley_20630 ELSE 0 END) AS prs_ret_ley_20630,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_ley_23351 ELSE 0 END) AS prs_ret_ley_23351,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_ret_ley_11265 ELSE 0 END) AS prs_ret_ley_11265,
				SUM(CASE RIGHT(t.origen,4) WHEN '_prs' THEN t.pag_cudaio ELSE 0 END) AS prs_cudaio,
				
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_pre_bruto ELSE 0 END) AS aprs_pre_bruto,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_pre_neto ELSE 0 END) AS aprs_pre_neto,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_total_sin_cudaio ELSE 0 END) AS aprs_ret_total_sin_cudaio,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_ley_20630 ELSE 0 END) AS aprs_ret_ley_20630,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_ley_23351 ELSE 0 END) AS aprs_ret_ley_23351,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_ret_ley_11265 ELSE 0 END) AS aprs_ret_ley_11265,
				SUM(CASE WHEN t.origen LIKE '%_aprs' THEN t.pag_cudaio ELSE 0 END) AS aprs_cudaio
			FROM tmp_tot t
			GROUP BY sor_pgmsorteo_id_c, tbl_provincias_id_c, pre_canal_de_pago_id_c 
			) tt
			INNER JOIN sor_pgmsorteo s ON s.id = tt.sor_pgmsorteo_id_c AND s.deleted=0
			INNER JOIN sor_producto p ON p.id_juego = s.idjuego AND p.deleted = 0
			INNER JOIN tbl_provincias pro ON pro.id = tt.tbl_provincias_id_c AND pro.deleted = 0
			-- LEFT JOIN  pre_resumen_emision r ON r.sor_pgmsorteo_id_c = t.sor_pgmsorteo_id_c AND r.tbl_provincias_id_c = t.tbl_provincias_id_c AND r.pre_canal_de_pago_id_c = t.pre_canal_de_pago_id_c
			-- WHERE r.sor_pgmsorteo_id_c IS NULL
			WHERE (COALESCE(s.sor_presc_recibida, 0) = (CASE WHEN s.idjuego IN (4,5,13,29) THEN 0 ELSE COALESCE(s.sor_presc_recibida, 0) END)
			       OR (tt.pre_canal_de_pago_id_c = 'O')
				)
			
	;
	DROP TEMPORARY TABLE IF EXISTS tmp_tot;
	
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_recalcula_resumen_emision - FIN ', id_pgmsorteo, ' id_consolidacion ', id_consolidacion));
				
    END$$

DELIMITER ;