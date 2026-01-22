DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `CC_Prescriptos_validacion`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `CC_Prescriptos_validacion`(IN id_proceso INT,IN id_secuencia INT,IN usuario VARCHAR(255), IN tipo_validacion CHAR(3),OUT msgret VARCHAR(2048))
fin:BEGIN
	
	DECLARE id_registros INT ;
	DECLARE id_md5 INT ;
	DECLARE registros_archivo_tabla INT;
	DECLARE registros_archivo_control INT;
	DECLARE dif_importes DECIMAL(18,2);
	DECLARE importe_archivo_tabla DECIMAL(18,2);
	DECLARE importe_archivo_control DECIMAL(18,2);
	DECLARE c_nro_liquidacion CHAR (10);
	DECLARE diferencia_cuit INT ;
	DECLARE id_importes INT ;
	
	DECLARE cuits INT ;
	
	DECLARE v_fecha_liquidacion DATE;
	DECLARE v_liquidacion INT;
	
	
	DECLARE v_cantidad_control INT;
	DECLARE v_cantidad INT;
	DECLARE v_importe DECIMAL(18,2);
	DECLARE v_importe_control DECIMAL(18,2);
	 
	INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - inicio ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	
		-- tmp account cstm
		DROP TEMPORARY TABLE IF EXISTS tmp_age_red; 
		CREATE TEMPORARY TABLE IF NOT EXISTS tmp_age_red (
		  tmp_id_red INT(6) DEFAULT NULL,
		  tmp_estado VARCHAR(100) DEFAULT NULL
		) ENGINE=INNODB DEFAULT CHARSET=utf8;
		
		-- tmp juego-sorteo-agencia-importe 
		DROP TEMPORARY TABLE IF EXISTS tmp_afectaciones_juego_sorteo; 
		CREATE TEMPORARY TABLE IF NOT EXISTS tmp_afectaciones_juego_sorteo (
		  juego INT(2) DEFAULT NULL,
		  sorteo INT(5) DEFAULT NULL,
		  agente INT(5) DEFAULT NULL,
		  id_concepto INT(2) DEFAULT NULL,
		  importe DECIMAL(15,2) DEFAULT NULL
		) ENGINE=INNODB DEFAULT CHARSET=utf8;
		
	-- realizar los controles indicados en la des funcional
	-- insertar las auditorias que se considrene necesarias
	-- ejemplo utilizar la variable msgret para devolver errores
    
	SET @cod_err = '0';	
	SET @usuario = usuario;  
	SET @id_proceso = id_proceso;
	SET @id_secuencia = id_secuencia;
	SET @fecha_hoy = DATE_FORMAT(CURDATE(), '%d%m%Y');
	
	-- actualiza el juego, 
	
			
			
	/*	VAIDACION DATOS	*/
	IF (tipo_validacion = 'DAT') THEN
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - inicio DAT ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	
		SET @contador_premios= 0;
		SELECT COUNT(p.id)  INTO @contador_premios
		FROM sor_rec_prescrip p ;
		
		SET @contador_ctrl= 0;
		SELECT  COUNT(c.id)  INTO @contador_ctrl
		FROM sor_rec_prescrip_ctrl c;
		
		IF (@contador_premios <= 0 || @contador_ctrl <= 0) THEN
		
			SET @cod_err = '1';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," No se volcaron tablas rec CC_Prescriptos_validacion");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
			SET msgret=CONCAT("Problema tablas no volcadas CC_Prescriptos_validacion.");
			LEAVE fin;-- sale del SP
		END IF;
	
	-- actualizo id_juego
		UPDATE sor_rec_prescrip
		SET nro_juego=	CASE  
					WHEN  nro_juego=14 THEN 4 -- quini6
					WHEN  nro_juego=10 THEN 50 -- loteria
					WHEN  nro_juego=20 THEN 51 -- loteria ch
					WHEN  nro_juego=24 THEN 49 -- loteria ch
					WHEN  nro_juego=33 THEN 5 -- loto
					WHEN  nro_juego=34 THEN 13 -- brinco
					WHEN  nro_juego=43 THEN 29 -- loto5
					WHEN  nro_juego=67 THEN 30 -- PF
					ELSE nro_juego
				END;	
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - ASIGNA ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	
	-- busca en la tabla de permiso
	-- actualiza campos a.cuit_calculado, a.permiso, a.id_cuenta, a.id_usuario, a.razon_social
		UPDATE sor_rec_prescrip   a
		INNER JOIN age_permiso b ON a.agente=b.numero_agente AND a.subagente=b.punto_venta_numero AND b.estado='activo'
		INNER JOIN age_personas p ON b.age_personas_id_c=p.id AND a.cuit=p.dni_cuit_titular_c
		INNER JOIN accounts_cstm cs ON b.account_id_c=cs.id_c 
						AND cs.numero_agente_c = a.agente AND cs.numero_subagente_c = a.subagente 
						AND cs.dni_cuit_titular_c = a.cuit
		INNER JOIN accounts aa ON aa.id=cs.id_c AND aa.deleted=0
		INNER JOIN users u ON u.user_name=cs.usuario_c 
		SET a.cuit_calculado=p.dni_cuit_titular_c,a.permiso=b.id_permiso,a.id_cuenta=b.account_id_c,a.id_usuario=u.id,a.razon_social=p.name;
		
				
		UPDATE sor_rec_prescrip   a
		INNER JOIN age_permiso b ON a.agente_originante=b.numero_agente AND a.subagente_originante=b.punto_venta_numero AND b.deleted=0	
		SET  a.permiso_originante=b.id_permiso;
	
	
	 -- busca en la tabla historica los datos al momento de la fecha del sorteo
		UPDATE sor_rec_prescrip a
		INNER JOIN accounts_cstm b ON a.agente=b.numero_agente_c 
						AND a.subagente=b.numero_subagente_c 
						AND a.vendedor=b.numero_vendedor_c
						-- AJUSTADO ADRIAN ENRICO 16/12/2015
						AND a.cuit = b.dni_cuit_titular_c
		INNER JOIN accounts ac ON b.id_c=ac.id AND ac.deleted=0
		INNER JOIN users u ON u.user_name=b.usuario_c  AND u.deleted=0
		
		SET  a.cuit_calculado=b.dni_cuit_titular_c,a.permiso=b.id_permiso_c,id_cuenta=b.id_c,a.id_usuario=u.id,a.razon_social=SUBSTRING_INDEX(ac.name,"-",-1)
		WHERE a.fecha_sorteo BETWEEN b.fecha_desde_c 
			AND DATE_SUB(COALESCE(b.fecha_hasta_c,'2100-01-01'), INTERVAL 1 DAY)
			AND (COALESCE(a.cuit_calculado,'')=''  OR a.cuit_calculado<>a.cuit); 	 	
		
	-- actualiza el permiso originante	
		UPDATE sor_rec_prescrip a
		INNER JOIN accounts_cstm b ON a.agente_originante=b.numero_agente_c AND a.subagente_originante=b.numero_subagente_c  AND a.vendedor_originante=b.numero_vendedor_c
		INNER JOIN accounts ac ON b.id_c=ac.id AND ac.deleted=0
			SET  a.permiso_originante=b.id_permiso_c
		WHERE a.fecha_sorteo BETWEEN fecha_desde_c AND DATE_SUB(COALESCE(b.fecha_hasta_c,'2100-01-01'), INTERVAL 1 DAY)
			AND  (COALESCE(a.cuit_calculado,'')='' OR a.cuit_calculado<>a.cuit);
	
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - VALIDA ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	
		-- insert id_registros, id_md5, v_cantidad, v_cantidad_control, id_importes, v_importe, v_importe_control,v_fecha_liquidacion,v_liquidacion	
		SELECT 	CASE  WHEN (COUNT(*) <> b.cantidad_registros) THEN 1  ELSE 0  END AS diferencia_registros,
				CASE  WHEN (b.md5_archivo <> b.md5_calculado) THEN 1 ELSE 0   END AS id_md5,	
				COUNT(*),
				b.cantidad_registros,
				CASE WHEN (SUM(a.importe) <> suma_importe)   THEN 1  ELSE 0  END AS diferencia_importes,
				SUM(a.importe),  
				b.suma_importe,
				b.fecha_liquidacion,
				b.nro_liquidacion
		INTO id_registros, 
			id_md5, 
			v_cantidad, 
			v_cantidad_control, 
			id_importes, 
			v_importe, 
			v_importe_control,
			v_fecha_liquidacion,
			v_liquidacion	
		FROM sor_rec_prescrip a
		INNER JOIN sor_rec_prescrip_ctrl b ON 1=1
		;
	
		SET id_registros = - 1 ;
		SELECT 
			CASE  WHEN (COUNT(*) <> cantidad_registros) THEN 1  ELSE 0  END AS diferencia_registros,
			CASE  WHEN (md5_archivo <> md5_calculado) THEN 1 ELSE 0   END AS id_md5, 
			COUNT(*),
			sor_rec_prescrip_ctrl.cantidad_registros, 
			CASE WHEN (SUM(importe) <> suma_importe)   THEN 1  ELSE 0  END AS diferencia_importes,
			SUM(importe),  
			sor_rec_prescrip_ctrl.suma_importe,    
			sor_rec_prescrip_ctrl.nro_liquidacion ,
			SUM(CASE WHEN (cuit<>COALESCE(cuit_calculado,0)) THEN 1 ELSE 0 END) AS diferencia_cuit
		INTO id_registros,    
			id_md5,   
			registros_archivo_tabla,    
			registros_archivo_control,    
			dif_importes,    
			importe_archivo_tabla,    
			importe_archivo_control,  
			c_nro_liquidacion,
			diferencia_cuit
		FROM    sor_rec_prescrip , sor_rec_prescrip_ctrl ;
	
		-- validaciones
		IF (id_registros > 0) THEN
			SET @cod_err = '2';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," No coincide la cantidad de registros informada con la cantidad de registros del archivo");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);  
			SET msgret = CONCAT("No coincide la cantidad de registros informada con la cantidad de registros del archivo. Registros informados: ",v_cantidad_control , " - Registros calculados: ",    v_cantidad) ;
			LEAVE fin;-- sale del SP
		END IF ;
		  
		  IF (id_md5 = 1)  THEN 
			SET @cod_err = '3';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," No coincide la huella digital informada con la calculada");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);   
			SET msgret = CONCAT( "No coincide la huella digital informada con la calculada.") ;
			LEAVE fin ;
			
		  END IF ;
		  
		  IF (dif_importes > 0)   THEN 
			SET @cod_err = '4';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," No coincide la suma calculada de importes con la informada");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);  
			SET msgret = CONCAT("No coincide la suma calculada de importes con la informada. Importe rec: ",   v_importe_control,    ". Importe calc: ",    v_importe  ) ;
			LEAVE fin ;
		  END IF ;
		
	
	-- Si hay cuit's con diferencias, tengo que buscar sobre los agentes dados de baja!
	SET cuits=-1;		
	SELECT COUNT(*)
		INTO 	cuits
		FROM sor_rec_prescrip a
		WHERE (a.cuit<>COALESCE(a.cuit_calculado,0));
	
	-- agentes dados de bajas
	IF cuits > 0 THEN
			
		DROP TEMPORARY TABLE IF EXISTS agentes_bajas;
		CREATE TEMPORARY TABLE agentes_bajas
			SELECT 	a.numero_agente_c AS  agente,
					a.numero_subagente_c AS subagente,
					u.id AS usuario ,
					a.dni_cuit_titular_c AS cuit, 
					a.id_c AS idcuenta,
					a.id_permiso_c AS permiso, 
					ac.name AS razonsocial 
				FROM accounts_cstm a
					INNER JOIN accounts ac ON a.`id_c`=ac.`id` AND ac.`deleted`=0
					INNER JOIN users u ON u.`user_name`=a.`usuario_c`
					INNER JOIN 
					(  
					SELECT   MAX(a.id_historico_as_c) AS id_historico -- ,u.`id` AS usuario,ac.name as razonsocial
						FROM accounts_cstm a
							-- INNER JOIN accounts ac ON a.`id_c`=ac.`id` AND ac.`deleted`=0
							-- INNER JOIN users u ON u.`user_name`=a.`usuario_c`
							INNER JOIN
								(
								SELECT ac.numero_agente_c, ac.numero_subagente_c, MAX(COALESCE(fecha_hasta_c, NOW())) AS fecha_hasta_c
									FROM accounts_cstm ac
									WHERE fecha_hasta_c IS NOT NULL
									GROUP BY ac.numero_agente_c, ac.numero_subagente_c
								) f ON a.`numero_agente_c`=f.numero_agente_c 
									AND a.`numero_subagente_c`=f.numero_subagente_c 
										AND a.`fecha_hasta_c`=f.fecha_hasta_c
						GROUP BY a.`numero_agente_c`,a.`numero_subagente_c`
						ORDER BY a.`numero_agente_c`,a.`numero_subagente_c`
					)sub ON a.id_historico_as_c=sub.id_historico;
		
		UPDATE sor_rec_prescrip a
			INNER JOIN agentes_bajas b  ON a.`agente`=b.agente AND a.`subagente`=b.subagente
				SET  a.cuit_calculado=b.cuit,a.permiso=b.permiso,id_cuenta=b.idcuenta,a.id_usuario=b.usuario,a.razon_social=b.razonsocial
			WHERE COALESCE(a.cuit_calculado,'')='' OR (a.cuit_calculado<>a.cuit);	
		
		UPDATE sor_rec_prescrip a
			INNER JOIN agentes_bajas b  ON a.agente_originante=b.agente AND a.subagente_originante=b.subagente
				SET   a.permiso_originante=b.permiso
			WHERE COALESCE(a.permiso_originante,'')='';		
  
	END IF   ;
	
		SELECT SUM(CASE WHEN (cuit<>COALESCE(cuit_calculado,0)) THEN 1 ELSE 0 END) AS diferencia_cuit
		INTO diferencia_cuit
		FROM sor_rec_prescrip a
		INNER JOIN sor_rec_prescrip_ctrl b ON 1=1;
	
			IF ( diferencia_cuit> 0)   THEN 
			
				
				DROP TEMPORARY TABLE IF EXISTS diferencias_agentes;
				CREATE TEMPORARY TABLE diferencias_agentes (
				  id VARCHAR(36),	
				  agente INT  DEFAULT NULL,
				  subagente INT DEFAULT 0,
				  permiso INT  DEFAULT 0,
				  cuit_archivo VARCHAR(13),
				  cuit_suite VARCHAR(13)
				  )  ;
		  
			
				INSERT INTO diferencias_agentes(id,agente,subagente,permiso,cuit_archivo,cuit_suite)
				SELECT UUID(),agente,subagente,permiso,cuit,cuit_calculado
				FROM sor_rec_afe  a
				WHERE (cuit<>COALESCE(cuit_calculado,0) OR COALESCE(cuit_calculado,0)=0)
				GROUP BY agente,subagente;
				-- se comenta ya que se presentas diferencias de cuit
				/*
				SET @cod_err = '5';
				SET msgret = CONCAT("Se presentan diferencias de C.U.I.T ") ;
				SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Procedimiento almacenado CC_Prescriptos_validacion");
				CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,545,@usuario,53,55, @msgaud);
				LEAVE fin;-- sale del SP
				*/
			END IF ;
  
	
	
	END IF;
	
	
	/*	VAIDACION SECUENCIA	*/
	IF (tipo_validacion = 'SEC') THEN
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - SEC ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		-- secuencia (liquidacion) volcada en tabla
		
		IF EXISTS (SELECT * FROM cas02_cc_prescripcion_cab c
					WHERE c.ac_liquidacion = @id_secuencia
					LIMIT 1) THEN
			SET @cod_err = '6';
			SET @msgaud = CONCAT("Codigo Error: ",@cod_err," Procedimiento almacenado CC_Prescriptos_validacion");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
			SET msgret = CONCAT("Validación: El archivo con la liquidacion ", @id_secuencia, " ya ha sido procesado...");
			LEAVE fin;-- sale del SP
		END IF;
	END IF;
	
	
	/*	VALIDA SI SORTEOS	*/
	IF (tipo_validacion = 'SOR') THEN
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - SOR ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		
		SELECT COUNT(p.id)  INTO @contador_diferencias_premios
		FROM sor_rec_prescrip p 
		LEFT JOIN sor_pgmsorteo s ON s.nrosorteo = p.nro_sorteo AND s.idjuego = p.nro_juego
		WHERE s.nrosorteo IS NULL;
		
		IF (@contador_diferencias_premios > 0) THEN
			SET @cod_err = '7';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Procedimiento almacenado CC_Prescriptos_validacion");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
			SET msgret=CONCAT("Problemas validando sorteos, se encontraron ", @contador_diferencias_premios, " diferencias con suite.");
			INSERT INTO kk_auditoria VALUES (CONCAT('cc_prescriptos_validacion - inicio ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion, ' hora ', NOW()));
			LEAVE fin;-- sale del SP
		END IF;
	
	END IF;
	
	
	/*	fecha liquidacion igual o superior a fecha de prescripcion del sorteo		*/
	IF (tipo_validacion = 'FLP') THEN
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - FLP ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	
			/*por las dudas si la validacion era entre el xml de control y el archivo de prescriptos*/
			
			/*
				SELECT COUNT(p.id) INTO @cont_fechas_liq
				FROM sor_rec_prescrip_ctrl c
				INNER JOIN sor_rec_prescrip p ON p.id_proceso = c.id_proceso
				WHERE
				DATE_FORMAT(c.fecha_liquidacion,'%Y-%m-%d') < DATE_FORMAT(p.fecha_sorteo,'%Y-%m-%d')
				;
			*/
	
			SELECT  COUNT(p.id) INTO @cont_fechas_liq
			FROM sor_rec_prescrip_ctrl c
			INNER JOIN sor_rec_prescrip p ON p.id_proceso = c.id_proceso
			INNER JOIN sor_pgmsorteo s ON s.nrosorteo = p.nro_sorteo AND s.idjuego = p.nro_juego
			WHERE -- s.nrosorteo IS NOT NULL -- quitar este filtro una vez puesto en testing ya que todos los sorteo deberían estar
			-- AND
			 DATE_FORMAT(c.fecha_liquidacion,'%Y-%m-%d') >= s.fecha_prescripcion -- validar con RL
			AND c.id_proceso = @id_proceso
			;
		
		IF (@cont_fechas_liq > 0) THEN
			SET @cod_err = '8';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," La fecha liquidación es inferior a fecha prescripto de sorteo");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
			SET msgret=CONCAT("La fecha liquidación es inferior a fecha prescripto de sorteo");
			LEAVE fin;-- sale del SP
		END IF;
		
		
	END IF;
	
	   /*		VALIDACIÓN AGENCIAS		*/
    
	IF (tipo_validacion = 'AGE') THEN
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - AGE ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
    
		INSERT INTO tmp_age_red(tmp_id_red, tmp_estado)
		SELECT id_red, estado FROM age_red;
			
			SELECT COALESCE((
				SELECT COUNT(c.id) FROM (
					SELECT p.id AS id , p.agente AS agente
					FROM sor_rec_prescrip p
					LEFT JOIN tmp_age_red cstm1 ON cstm1.tmp_id_red = p.agente
					WHERE cstm1.tmp_id_red IS NULL
					GROUP BY  p.agente
				) c	
			),0) AS cant_age  INTO @cant_age;
			
			IF (@cant_age > 0) THEN
				SET @cod_err = '9';
				SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Se detectan ", @cant_age, " agencias no registradas en suite");
				CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
				SET msgret="Se detectan diferencias de agencias en archivo sor_rec_prescrip.";
				LEAVE fin;-- sale del SP
			END IF;
	
	END IF;
	
	/*	 VALIDA CONCEPTOS		*/
	IF (tipo_validacion = 'CON') THEN
	
		INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - CON ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
		
		-- SET v_cantidad =0;
		
		/*	
		-- se comenta ya que en el archivo vienen diferentes conceptos (15,17, 25,30, 99)
		SELECT  COUNT(*) INTO @v_cantidad
		FROM sor_rec_prescrip a
		WHERE a.codigo_concepto NOT IN(15,17)  AND a.codigo_concepto<>99;
		
			
		IF (@v_cantidad>0)  THEN
			SET @cod_err = '10';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Se detectan ", @conceptos, " registros que no contienen ningun concepto de prescripciones");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,546,@usuario,53,55, @msgaud);
			SET msgret="Existen registros que no contienen concepto de prescripciones.";
			LEAVE fin;-- sale del SP
		END IF ;
		*/	
		
		SELECT COUNT(p.id)  INTO @conceptos
			FROM sor_rec_prescrip p 
				LEFT JOIN cas02_cc_conceptos c ON c.co_id = p.codigo_concepto
			WHERE c.co_id IS NULL;
		
		IF (@conceptos > 0) THEN
			SET @cod_err = '11';
			SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Se detectan ", @conceptos, " conceptos no registrados en suite");
			CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
			SET msgret="Se detectan conceptos en sor_rec_prescrip que no están en suitecrm.";
			LEAVE fin;-- sale del SP
		END IF;
		
		--  agregado de validaciones de conceptos abs(4) -  abs(17) = abs(15)
		
		DROP TEMPORARY TABLE IF EXISTS juegosorteo; 
		CREATE TEMPORARY TABLE IF NOT EXISTS juegosorteo (
		  juego INT DEFAULT NULL,
		  sorteo INT DEFAULT NULL,
		  id_pgmsorteo CHAR(36) DEFAULT NULL,
		--  concepto int default null,
		  importe_concept4 DECIMAL(18,2) DEFAULT NULL,
		  importe_concept15 DECIMAL(18,2) DEFAULT NULL,
		  importe_concept17 DECIMAL(18,2) DEFAULT NULL,
		   PRIMARY KEY (id_pgmsorteo,juego,sorteo)
		) ENGINE=INNODB DEFAULT CHARSET=utf8;
		DROP TEMPORARY TABLE IF EXISTS juegosorteo2; 
		CREATE TEMPORARY TABLE IF NOT EXISTS juegosorteo2 (
		  juego INT DEFAULT NULL,
		  sorteo INT DEFAULT NULL,
		  id_pgmsorteo CHAR(36) DEFAULT NULL,
		PRIMARY KEY (id_pgmsorteo,juego,sorteo)
		) ENGINE=INNODB DEFAULT CHARSET=utf8;
		
			 -- obtener juego- sorteo del archivo volcado
	 -- pre_pagaagencia = 'S' de pre_premios
		INSERT INTO juegosorteo(juego, sorteo,id_pgmsorteo, importe_concept15,importe_concept17,importe_concept4)
		SELECT 	a.nro_juego AS juego,
				a.nro_sorteo AS sorteo,
				a.id_pgmsorteo,
				SUM(a.importe_codconcp_15) AS importe_concept15, 
				SUM(a.importe_codconcp_17) AS importe_concept17, 
				SUM(a.importe_codconcp_4) AS importe_concept4
		FROM(
			 SELECT 	p.nro_juego AS nro_juego, 
					p.nro_sorteo AS nro_sorteo,
					s.id AS id_pgmsorteo,
					p.codigo_concepto AS codigo_concepto, 
					ABS(SUM(CASE WHEN p.codigo_concepto = 15 THEN COALESCE((p.importe),0) ELSE 0 END)) AS importe_codconcp_15, 
					ABS(SUM(CASE WHEN p.codigo_concepto = 17 THEN COALESCE((p.importe),0) ELSE 0 END)) AS importe_codconcp_17,
					ABS(SUM(CASE WHEN p.codigo_concepto = 4 THEN COALESCE((p.importe),0) ELSE 0 END)) AS importe_codconcp_4
			 FROM sor_rec_prescrip p
			 INNER JOIN sor_pgmsorteo s ON s.idjuego = p.nro_juego AND s.nrosorteo = p.nro_sorteo
			 INNER JOIN pre_premios pr ON pr.sor_pgmsorteo_id_c = s.id  AND pr.pre_pagaagencia = 'S' -- premios paga agencia
			 WHERE p.codigo_concepto IN (15,17,4) AND p.id_proceso = @id_proceso
			 GROUP BY p.nro_juego, p.nro_sorteo,p.codigo_concepto 
		) a
		GROUP BY a.nro_juego,a.nro_sorteo;
		
		INSERT INTO juegosorteo2(juego, sorteo,id_pgmsorteo)
		SELECT j.juego, j.sorteo,j.id_pgmsorteo FROM juegosorteo j ;	
		
	-- calculando concepto 4 
	 UPDATE juegosorteo js
		 INNER JOIN (SELECT  	
					ju.id_juego AS juego, 
					c.ac_sorteo AS sorteo,
					-- 4 AS codigo_concepto, 
					0 AS importe_codconcp_15, 
					0 AS importe_codconcp_17,
					ABS(SUM(CASE WHEN co.co_id=4 THEN COALESCE((a.ad_importe),0) ELSE 0 END)) AS importe_codconcp_4 
				  FROM cas02_cc_afectacion_cab c   
					 INNER JOIN cas02_cc_afectacion_det_c b ON c.id=b.cas02_cc_ab25bion_cab_ida AND c.deleted=0  AND c.ac_duplicado=0 AND b.deleted=0
					 INNER JOIN cas02_cc_afectacion_det a ON a.id=b.cas02_cc_a3529ion_det_idb AND b.deleted=0 
					 INNER JOIN accounts ac ON c.account_id_c=ac.id AND ac.deleted=0 
					 INNER JOIN accounts_cstm acs ON acs.id_c=ac.id 
					 INNER JOIN cas02_cc_conceptos co ON co.id=a.cas02_cc_conceptos_id_c AND co.deleted=0 
					 INNER JOIN sor_producto ju ON ju.id=c.cas02_cc_juegos_id_c AND ju.deleted=0 
					 INNER JOIN juegosorteo2 jj ON ju.id_juego = jj.juego AND c.ac_sorteo= jj.sorteo
				 GROUP BY ju.id_juego, c.ac_sorteo
				 ORDER BY ju.id_juego, c.ac_sorteo
				 ) a ON a.juego = js.juego AND a.sorteo = js.sorteo
			SET js.importe_concept4 = a.importe_codconcp_4;
 
 
		 SELECT COUNT(*) INTO @resultado
		 FROM (
				SELECT 
					CASE WHEN (importe_concept4 - importe_concept17 = importe_concept15 ) THEN 0 ELSE 1 END AS valida
				FROM juegosorteo
		) a  WHERE a.valida = 1 ;
	
		IF (@resultado > 0) THEN
				SET @cod_err = '12';
				SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Se detectan ", @resultado, " inconsistencias en cuadratura total");
				CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
				SET msgret="No cuadra total prescripto (conc. 15) con total liquidado (conc. 4) y total pagado (conc. 17)";
				LEAVE fin;
		END IF;
		
	
		SELECT COUNT(*)  INTO @resultado
		 FROM (	 
			SELECT CASE WHEN (ABS(SUM(r.pag_pre_neto)) = j.importe_concept17) AND r.pre_canal_de_pago_id_c = 'A' THEN 0 ELSE 1 END valida
			FROM juegosorteo j 
			INNER JOIN  pre_resumen_emision r  ON r.sor_pgmsorteo_id_c = j.id_pgmsorteo  
			GROUP BY j.id_pgmsorteo 
		) a  WHERE valida = 1; 
	
		IF (@resultado > 0) THEN
				SET @cod_err = '13';
				SET @msgaud=CONCAT("Codigo Error: ",@cod_err," Se detectan ", @resultado, " inconsistencias en concepto de premios");
				CALL sor_inserta_auditoria(@id_proceso,999, @id_secuencia,202,@usuario,201,203, @msgaud);
				SET msgret="No coincide el concepto de premios pagados por agencias (17) con los pagos procesados diariamente (archivo pagos). Verifique que se hayan procesado todos los pagos a la fecha de prescripción y vuelva a intentar";
				LEAVE fin; 
		END IF;
		
	END IF;
    
	/*
	 IF (tipo_validacion = 'PRE') THEN
	
		CALL CC_Prescriptos_Resumen(@id_proceso, @id_secuencia,@usuario, tipo_validacion, @msgretVuelco);
		
		IF (@msgretVuelco != "OK") THEN
		
			SET @cod_err = '8';
			SET msgret= CONCAT(@msgretVuelco,". Envío Nro: ",@id_secuencia);
			SET @msgaud=CONCAT(@msgretVuelco,". Envío Nro: ",@id_secuencia);
			-- inserta auditoria
			CALL sor_inserta_auditoria(@id_proceso,-999, @id_secuencia,548,@usuario,54,56, @msgaud);
		
		END IF;
	
	END IF;
     */
     
     IF (@cod_err != '0')  THEN
	SET msgret="ER";
	-- CALL sor_inserta_auditoria(@id_proceso,-999, @id_secuencia,30,@usuario,0,28, @msgaud);
	-- SET msgret=concat('Problemas de validaciones',@secuencia_enviada, @secuencia_esp, @secuencia_ant );
	INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), ' cc_prescriptos_validacion - fin con error ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	LEAVE fin;-- sale del SP
     ELSE
	-- inserta auditoria fin de proceso
	 SET msgret="OK";
	--  SET @msgaud="Finalización correcta de proceso recepción de liquidaciones";
	--  CALL sor_inserta_auditoria(@id_proceso,-999, @id_secuencia,28,@usuario,0,0, @msgaud);
	INSERT INTO kk_auditoria VALUES (CONCAT(NOW(), 'cc_prescriptos_validacion - fin ok ', id_proceso, ' secuencia ', id_secuencia, ' usuario ', usuario, ' tipo_validacion ', tipo_validacion));
	LEAVE fin;-- sale del SP
    END IF;
		
		
        
   
    END$$

DELIMITER ;