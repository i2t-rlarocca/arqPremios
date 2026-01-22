DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `sor_publica_minutas`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `sor_publica_minutas`(
									IN par_juego INT, -- puede venir un -1 significando CUALQUIERA
									IN par_sorteo INT, -- puede venir un -1 significando CUALQUIERA
									IN usuario CHAR(36),
									IN par_proceso VARCHAR(15), -- identifica el proceso que tiene que resolver
									OUT RCode INT,
									OUT RTxt LONGTEXT,
									OUT RId	VARCHAR(36),
									OUT RSQLErrNo INT,
									OUT RSQLErrtxt VARCHAR(2048)
    )
    COMMENT 'Genera minutas contables'
fin:BEGIN
/*
***************************************************************************************************************************************************************
Este proceso se invoca desde:
- el proceso de tratamiento de pagos de premios menores (los que realiza la agencia), que envia un proceso de tipo 'ppag' con un id que es la fecha del archivo
  y toma las minutas del grupo 'ppag'!
- el proceso de vuelco de premios de qnl extra, que envia un proceso de tipo 'lqng' con un id que es juego / sorteo y toma las minutas del grupo 'lqng'!
- el proceso impresión de minutas de la ctacte provincias -> hoy desde CAS02_CC_VTASPCIA_RESERVA, que envia un proceso de tipo cc_p y un id que es juego / sorteo
  y toma las minutas del grupo 'cc_p'
- el proceso de tratamiento de ctacte (recepcion del cas.zip) 'cc_a'
- el proceso de cierre de sorteo 'ppre'
***************************************************************************************************************************************************************
*/        
	DECLARE id_registros INT ;
	DECLARE hubo_problemas BOOLEAN DEFAULT FALSE;
	
	DECLARE var_quini6 INT DEFAULT 4;
	DECLARE var_brinco INT DEFAULT 13;
	DECLARE var_poceadafederal INT DEFAULT 30;
	DECLARE id_pgmsorteo, id_producto CHAR(36);
	DECLARE p_minuta, p_orden, p_impcbl, p_signo_a, p_minuta_a, p_impcbl_a, p_signo_b, p_minuta_b, p_impcbl_b, p_id_jgoas400 INT;
	DECLARE p_debehaber, p_debehaber_a, p_debehaber_b CHAR(1);
	DECLARE p_query TEXT;
		
	DECLARE done INT DEFAULT 0;
	DECLARE parametros CURSOR FOR 
		SELECT par.par_minuta, par.par_debehaber, par.par_orden, par.par_impcbl, par.par_query, 
				par.par_signo_a, par.par_minuta_a, par.par_impcbl_a, par.par_debehaber_a,
				par.par_signo_b, par.par_minuta_b, par.par_impcbl_b, par.par_debehaber_b, par.sor_producto_id_c, p.id_as400
			FROM sor_rec_ctaspcia_parametros par
				JOIN sor_rec_ctaspcia_minutas m ON m.sor_producto_id_c = par.sor_producto_id_c AND m.min_id = par.par_minuta
				JOIN sor_producto p ON p.id = par.sor_producto_id_c
			WHERE m.min_grupo = LEFT(par_proceso, 4) -- son las del grupo
				AND (p.id_juego = par_juego OR par_juego = '-1') -- y del juego (salvo que juego sea -1!)
				AND par.par_activo = 'S'
			ORDER BY par.sor_producto_id_c, par.par_minuta, par.par_debehaber, par.par_orden
			;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN	
		-- Retorno ok
		
		GET DIAGNOSTICS CONDITION 1
		 	@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
			@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
		
		-- SET @code = 1, @errno = 1, @msg = 'Error...', @base='', @tabla='';
		IF (@trans = 1) THEN
			ROLLBACK;
		END IF;
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - FIN CON ERRORES - juego ', par_juego, 
							' sorteo ', par_sorteo, ' proceso ', par_proceso, '- etapa: ' , @etapa, 
							' - Error ', COALESCE(@errno, 0), ' Mensaje ', COALESCE(@msg, '')));
	    
		SET 	RCode = @code,
			RTxt = "Error SQL en publicación de minutas",
			RId = 0,
			RSQLErrNo = @errno,
			RSQLErrtxt = CONCAT(@msg, ' en tabla ', @tabla, ' de ', @base, '  - llegó a etapa: ', @etapa) ;
			
	END;
	
	SET id_registros = - 1,
	    @etapa = 0,
	    @trans = 0,
	    @grupo_minuta = LEFT(par_proceso,4)
	    ;
	SET RCode = 0;
	SET RTxt = 'OK';
	SET RId = '0';
	SET RSQLErrNo = 0;
	SET RSQLErrtxt = "OK";
	
	-- ***** Si el proceso es cc_p: Verificamos concurrencia particular en el proceso 
	IF (@grupo_minuta = 'cc_p') THEN
		-- *****   CONTROL DE CONCURRENCIA: SI NO EXISTE O EXISTE UNA EN CURSO pero es del mismo sorteo CONTINUO. SI EXISTE PERO ES DE OTRO SORTEO CANCELO!!
		SET @RId = '0';
		CALL SP_ET_ConcurrenciaGET2('1','1','cas02_cc_parametros',
			@RCode,
			@RTxt,
			@RId,
			@RSQLErrNo,
			@RSQLErrtxt
		);
		IF (@RId != '0') THEN
			-- Busco el ultimo proceso que tiene concurrencia. 
			--    Si el id_registro_concurrencia de ese ultimo proceso coincide con el sorteo que estoy procesando continúo sino cancelo
			SET @id_registro_concurrencia = NULL;
			SELECT COALESCE(a.description,0) INTO @id_registro_concurrencia
			FROM sor_aud_consolidacion a 
			WHERE a.id_pro_consolidacion = 
				(SELECT MAX(b.id_pro_consolidacion) 
				FROM sor_aud_consolidacion  b
				WHERE COALESCE(b.description,'') != ''
				)
			;
			IF !(COALESCE(@id_registro_concurrencia,0) = par_juego*1000000+par_sorteo) THEN
				SET @msgret='CONCURRENCIA: Existe un proceso de afectaciones de juegos poceados en curso. Intente nuevamente en unos instantes.'; 
				SET 	RCode = '-1',
					RTxt = @msgret,
					RId = @RId,
					RSQLErrNo = '0',
					RSQLErrtxt = ''
				;
				INSERT INTO kk_auditoria2 (nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - Concurrencia particular cc_p - FIN por existencia de concurrencia - @id_registro_concurrencia: ',@id_registro_concurrencia,' - juego: ',par_juego, ' - sorteo: ',par_sorteo,' - RId: ', RId, ' - RTxt: ', RTxt));
				LEAVE fin ;	
			END IF;
		END IF;
		-- *****   FIN CONTROL DE CONCURRENCIA: SI NO EXISTE O EXISTE UNA EN CURSO pero es del mismo sorteo CONTINUO. SI EXISTE PERO ES DE OTRO SORTEO CANCELO!!
	END IF;
	-- ***** FIN Si el proceso es cc_p: Verificamos concurrencia particular en el proceso 
	
	-- ***** Verificamos concurrencia general en el proceso 
	INSERT INTO kk_auditoria2 (nombre)VALUES(CONCAT(NOW(), ' sor_publica_minutas - INICIO - CALL SP_ET_ConcurrenciaINS2 en tabla cas02_cc_grupo_minutas.'));	
	-- CALL SP_ET_ConcurrenciaINS2('cas02_cc_grupo_minutas',usuario,'cc_a',3600,0,@RCode,@RTxt,@RId,@RSQLErrNo,@RSQLErrtxt);
	CALL SP_ET_ConcurrenciaINS2('cas02_cc_grupo_minutas',usuario,'cc_a', 1800,0, 
		@RCode,
		@RTxt,
		@RId,
		@RSQLErrNo,
		@RSQLErrtxt
	);
	SET @concurr_gral = @RId;
	IF (@RCode != 1) THEN
		IF (@RCode = -3) THEN
			SET RTxt = 'CONCURRENCIA general: Existe un proceso de afectaciones de juegos poceados en curso. Intente nuevamente en unos instantes.'; 
		ELSE
			SET RTxt = 'CONCURRENCIA: Se ha producido una excepción al intentar generar la concurrencia general. Intente nuevamente en unos instantes.'; 
		END IF;
		SET RCode = -1;
		SET RId = @RId;
		SET RSQLErrNo = @RSQLErrNo;
		SET RSQLErrtxt = @RSQLErrtxt;		
		INSERT INTO kk_auditoria2 (nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - Concurrencia general - FIN ERROR CALL SP_ET_ConcurrenciaINS2 - ', @RTxt));				
		IF (@grupo_minuta = 'cc_p') THEN
				-- *****   CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!
				SET @RId = '0';
				CALL SP_ET_ConcurrenciaGET2('1','1','cas02_cc_parametros', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
				IF (@RId != '0') THEN
					SET @id_registro_concurrencia = NULL;
					SELECT COALESCE(a.description,0) INTO @id_registro_concurrencia
					FROM sor_aud_consolidacion a 
					WHERE a.id_pro_consolidacion = 
						(SELECT MAX(b.id_pro_consolidacion) 
						FROM sor_aud_consolidacion  b
						WHERE b.idjuego = par_juego AND b.nrosorteo = par_sorteo AND COALESCE(b.description,'') != ''
						)
					;
					IF (COALESCE(@id_registro_concurrencia,0) = par_juego*1000000+par_sorteo) THEN
						-- ***** LIBERO CONCURRENCIA !!!
						SET @id_concurrencia = @RId;
						SET @RId = '0';
						CALL SP_ET_ConcurrenciaDEL2(@id_concurrencia, 'N', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
						UPDATE sor_aud_consolidacion
						SET description = NULL
						WHERE description = @id_registro_auditoria
						;
					END IF;
				END IF;
				-- *****   FIN CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!	    
			END IF;		
		LEAVE fin ;
	END IF;
	INSERT INTO kk_auditoria2 (nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - Concurrencia general - FIN ok CALL SP_ET_ConcurrenciaINS2 - id_concurrencia: ', @RId));
	-- ***** FIN Verificamos concurrencia general en el proceso 	
	
	-- 
	-- segun @grupo_minuta, considera utilizar fecha de pago para ppag. se condicionan scripts
	--
	
	DROP TABLE IF EXISTS tmp_cas02_cc_cntpoc_det; 
	IF @grupo_minuta = 'ppag' THEN
		CREATE  
		TABLE IF NOT EXISTS tmp_cas02_cc_cntpoc_det (
				`minuta` 		INT(3) NOT NULL,
				`debehaber` 		CHAR(1) NOT NULL, 
				`impcbl`    		INT(11) NOT NULL,
				`orden`     		INT(3) NOT NULL,
				`importe`   		DECIMAL(13,2) DEFAULT 0,
				`importea`  		DECIMAL(13,2) DEFAULT 0,
				`importeb`  		DECIMAL(13,2) DEFAULT 0,
				`sor_producto_id_c`  	CHAR(36) DEFAULT '',
				`sor_pgmsorteo_id_c` 	CHAR(36) DEFAULT '',
				`fecha_pago` 		DATE,
				PRIMARY KEY (`minuta`, `impcbl`, `sor_producto_id_c`, `sor_pgmsorteo_id_c`, `debehaber`, fecha_pago)
		) ENGINE=INNODB DEFAULT CHARSET=utf8;
	ELSE
		CREATE  
		TABLE IF NOT EXISTS tmp_cas02_cc_cntpoc_det (
				`minuta` 		INT(3) NOT NULL,
				`debehaber` 		CHAR(1) NOT NULL, 
				`impcbl`    		INT(11) NOT NULL,
				`orden`     		INT(3) NOT NULL,
				`importe`   		DECIMAL(13,2) DEFAULT 0,
				`importea`  		DECIMAL(13,2) DEFAULT 0,
				`importeb`  		DECIMAL(13,2) DEFAULT 0,
				`sor_producto_id_c`  	CHAR(36) DEFAULT '',
				`sor_pgmsorteo_id_c` 	CHAR(36) DEFAULT '',
				PRIMARY KEY (`minuta`, `impcbl`, `sor_producto_id_c`, `sor_pgmsorteo_id_c`, `debehaber`)
		) ENGINE=INNODB DEFAULT CHARSET=utf8;
	END IF;
	    
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), " sor_publica_minutas - INICIO  juego ", par_juego, 
							" sorteo ", par_sorteo, " proceso ", par_proceso, "- etapa: " , @etapa));
	    
        -- #######################################################################################################################################
        -- Validaciones
        -- #######################################################################################################################################
        SET RCode = 0;
        
	-- Que el juego exista
	SET @etapa = @etapa + 1; -- 1
/*      el juego se cruzó contra la minuta!
	SET @id_producto = -1;
	SELECT p.id INTO @id_producto
		 FROM sor_producto p
		 WHERE p.id_juego = par_juego
	; 
	IF (@id_producto < 0) THEN
	        SET 	RCode = 1, 
			RTxt = CONCAT("No existe el juego ", par_juego);
	END IF;
*/	
        -- SI MODO CTA.CTE.PCIAS -> Que exista modalidad tradicional! - codigo_modalidad = 1
	SET @etapa = @etapa + 1; -- 2 
	SET @id_trad = -1;
	IF (@grupo_minuta = 'cc_p') THEN
		SELECT m.id INTO @id_trad
			 FROM sor_producto p
				JOIN sor_modalidades m ON m.sor_producto_id_c = p.id
			 WHERE p.id_juego = par_juego AND m.activa = 1 AND m.codigo_modalidad = 1
		; 
		IF (@id_trad < 0) THEN
			SET 	RCode = 2, 
				RTxt = CONCAT("No existe modalidad tradicional activa para el juego ", par_juego);
		END IF;
	END IF;
	-- Que el sorteo exista y se haya procesado (estado >= 50!)
	SET @etapa = @etapa + 1; -- 3
	SET @id_pgmsorteo = -1, @alea = 'NO';
	IF (par_juego <> -1 AND par_sorteo <> -1) THEN
		SELECT ps.id, MAX(IF(par_juego != var_quini6 OR a.id IS NULL, 'NO', 'SI'))
			INTO @id_pgmsorteo, @alea
			 FROM sor_pgmsorteo ps
				LEFT JOIN sor_acciones a ON a.fecha = ps.fecha AND a.tipo_accion = 'ALEA' AND a.deleted = 0
			 WHERE ps.idjuego = par_juego AND ps.nrosorteo = par_sorteo
		; 
		IF (@id_pgmsorteo < 0) THEN
			SET 	RCode = 3, 
				RTxt = CONCAT("No existe programa de sorteo para el juego ", par_juego, 
								" sorteo ", par_sorteo, " proceso ", par_proceso);
		END IF;
		
		IF (@grupo_minuta = 'cc_p' AND @alea = 'SI') THEN
			SET 	RCode = 99, 
				RTxt = CONCAT("OK. Es un SORTEO FEDERAL (ALEA), se difiere la minuta ", par_juego, 
								" sorteo ", par_sorteo, " proceso ", par_proceso);
		END IF;
	END IF;
	-- Que el sorteo sea el siguiente al ultimo procesado
	-- Que el registro de reservas del juego/sorteo exista y este en estado de procesamiento PENDIENTE o PROVISORIO
	IF (RCode > 0) THEN
	 	SET     RId = 0,
			RSQLErrNo = 0,
			RSQLErrtxt = "";
		IF (@grupo_minuta = 'cc_p') THEN
				-- *****   CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!
				SET @RId = '0';
				CALL SP_ET_ConcurrenciaGET2('1','1','cas02_cc_parametros', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
				IF (@RId != '0') THEN
					SET @id_registro_concurrencia = NULL;
					SELECT COALESCE(a.description,0) INTO @id_registro_concurrencia
					FROM sor_aud_consolidacion a 
					WHERE a.id_pro_consolidacion = 
						(SELECT MAX(b.id_pro_consolidacion) 
						FROM sor_aud_consolidacion  b
						WHERE b.idjuego = par_juego AND b.nrosorteo = par_sorteo AND COALESCE(b.description,'') != ''
						)
					;
					IF (COALESCE(@id_registro_concurrencia,0) = par_juego*1000000+par_sorteo) THEN
						-- ***** LIBERO CONCURRENCIA !!!
						SET @id_concurrencia = @RId;
						SET @RId = '0';
						CALL SP_ET_ConcurrenciaDEL2(@id_concurrencia, 'N', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
						UPDATE sor_aud_consolidacion
						SET description = NULL
						WHERE description = @id_registro_auditoria
						;
					END IF;
				END IF;
				-- *****   FIN CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!	    
			END IF;	
		LEAVE fin;
	END IF;
	
        -- #######################################################################################################################################
        -- Procesamiento
        -- #######################################################################################################################################
        -- Abro la tabla de parametros para generacion de minutas...
        
	OPEN parametros;
	SET @etapa = @etapa + 1; -- 4
	SET @i = 0;
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - abro el cursor - etapa: ', @etapa));
	par_loop: LOOP
		FETCH parametros INTO p_minuta, p_debehaber, p_orden, p_impcbl, p_query, 
					p_signo_a, p_minuta_a, p_impcbl_a, p_debehaber_a, p_signo_b, p_minuta_b, p_impcbl_b, p_debehaber_b, id_producto, p_id_jgoas400;
		-- Si se terminó el cursor, salgo!
		IF (`done` = 1) THEN 
			LEAVE par_loop; 
		END IF; 
		SET @i = @i + 1;
		
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), '-',@i, ' sor_publica_minutas - nuevo registro inicio - etapa: ', @etapa, 
								' id:', p_minuta, '-', p_id_jgoas400, '-', p_debehaber, '-', p_orden, '-', p_impcbl, 
								'->', COALESCE(p_impcbl_a,''), '_', COALESCE(p_signo_a, ''), '_', COALESCE(p_debehaber_a, ''),
								'->', COALESCE(p_impcbl_b,''), '_', COALESCE(p_signo_b, ''), '_', COALESCE(p_debehaber_b, ''))
								);
		SET @importe_a=0, @importe_b=0;
		-- Si existe, busco minutas previas
		
		IF (p_impcbl_a <> 0 AND p_signo_a <> 0 AND p_debehaber_a IN ('D', 'H')) THEN
			SELECT ROUND((d.importe + d.importea + d.importeb) * p_signo_a, 2) INTO @importe_a
				FROM tmp_cas02_cc_cntpoc_det d
				WHERE d.minuta = p_minuta_a AND d.impcbl = p_impcbl_a AND d.debehaber = p_debehaber_a;
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), '-',@i, ' sor_publica_minutas - incorporo importe a - etapa: ', @etapa, ' id:', p_minuta, '-', p_debehaber_a, '-', p_orden, ' importe a:', COALESCE(@importe_a,0),' p_impcbl_a: ', COALESCE(p_impcbl_a,'')));
		END IF;
		IF (p_impcbl_b <> 0 AND p_signo_b <> 0  AND p_debehaber_b IN ('D', 'H')) THEN
			SELECT ROUND((d.importe + d.importea + d.importeb)  * p_signo_b, 2) INTO @importe_b
				FROM tmp_cas02_cc_cntpoc_det d
				WHERE d.minuta = p_minuta_b AND d.impcbl = p_impcbl_b AND d.debehaber = p_debehaber_b;
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), '-',@i, ' sor_publica_minutas - incorporo importe b - etapa: ', @etapa, ' id:', p_minuta, '-', p_debehaber_b, '-', p_orden, ' importe b:', COALESCE(@importe_a,0),' p_impcbl_b: ', COALESCE(p_impcbl_b,'')));
		END IF;
		
		-- ------------------------------------------------------------------------------------------------------------------------
		
		-- Preparo la sentencia
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), '-',@i,  ' sor_publica_minutas - nuevo registro calculo - etapa: ', @etapa, ' q:', p_query));
		SET @query = REPLACE(p_query, '[MINUTA]', p_minuta);
		SET @query = REPLACE(@query, '[DEHA]', CONCAT('"', p_debehaber, '"'));
		SET @query = REPLACE(@query, '[ORDEN]', p_orden);
		SET @query = REPLACE(@query, '[IMPCBL]', p_impcbl);
		SET @query = REPLACE(@query, '[JUEGO]', par_juego);
		SET @query = REPLACE(@query, '[SORTEO]', par_sorteo);
		SET @query = REPLACE(@query, '[TRADICIONAL]', CONCAT('"', @id_trad, '"'));
		SET @query = REPLACE(@query, '[IMPORTEA]', @importe_a);
		SET @query = REPLACE(@query, '[IMPORTEB]', @importe_b);
		SET @query = REPLACE(@query, '[ID_PRODUCTO]', CONCAT('"', id_producto, '"'));
		SET @query = REPLACE(@query, '[ID_JGOAS400]', p_id_jgoas400);
		SET @query = REPLACE(@query, '[ID_PGMSORTEO]', CONCAT('"', @id_pgmsorteo, '"'));
		SET @query = REPLACE(@query, '[IDENPRC]', CONCAT('"', par_proceso, '"'));
		SET @query = CONCAT('Insert into tmp_cas02_cc_cntpoc_det ', @query);
		-- La ejecuto
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), '-',@i,  ' sor_publica_minutas - nuevo registro fin - etapa: ', @etapa, ' q:', @query, ' importe_a: ',@importe_a, ' importe_b: ', @importe_b));
		PREPARE sentenciasql FROM @query; 
		EXECUTE sentenciasql;
		-- se prende durante las posibles lecturas fallidas -----------------------------------------------------------------------
		SET done = 0;
		
	END LOOP par_loop;
	
	SET @etapa = @etapa + 1; -- 5
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - cierro el cursor - etapa: ', @etapa));
	CLOSE parametros;
	-- Doy vuelta los importes negativos!!!
	SET @etapa = @etapa + 1; -- 6
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - doy vuelta los valores negativos - etapa: ', @etapa));
	/* por ahora no, vemos despues si es necesario
	update tmp_cas02_cc_cntpoc_det d
		set d.importe  = d.importe  * -1,
		    d.importea = d.importea * -1,
		    d.importeb = d.importeb * -1,
		    d.debehaber = IF(d.debehaber = 'D', 'H', 'D')
		where d.minuta > 0 and (d.importe + d.importea + d.importeb) < 0;
	*/
	SET @etapa = @etapa + 1; -- 7
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - verifico cuadratura - etapa: ', @etapa));
	-- Control de minutas, deben cuadrar!
	IF EXISTS(SELECT d.sor_producto_id_c, d.sor_pgmsorteo_id_c, d.minuta, 
			SUM(IF(d.debehaber = 'D', d.importe + d.importea + d.importeb, 0)) AS total_debe, 
			SUM(IF(d.debehaber = 'H', d.importe + d.importea + d.importeb, 0)) AS total_haber
				FROM tmp_cas02_cc_cntpoc_det d
				WHERE d.minuta > 0
				GROUP BY d.minuta, d.sor_producto_id_c, d.sor_pgmsorteo_id_c
				HAVING total_debe <> total_haber) THEN
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - diferencia en control de cuadratura. Juego:', par_juego, ' Sorteo:', par_sorteo, " Proceso:", par_proceso));
			INSERT INTO kk_auditoria2(nombre) 
				SELECT CONCAT(NOW(), ' sor_publica_minutas - diferencias - minuta :', dif.minuta, 
									'juego:', dif.juego, ' sorteo:', dif.sorteo, 
									' debe:', dif.total_debe, ' haber:', dif.total_haber ) AS txt
					FROM (SELECT d.minuta AS minuta, COALESCE(p.name, 'n/a') AS juego, COALESCE(ps.name, 'n/a') AS sorteo,
								SUM(IF(d.debehaber = 'D', d.importe + d.importea + d.importeb, 0)) AS total_debe, 
								SUM(IF(d.debehaber = 'H', d.importe + d.importea + d.importeb, 0)) AS total_haber
							FROM tmp_cas02_cc_cntpoc_det d
								LEFT JOIN sor_producto p ON p.id = d.sor_producto_id_c
								LEFT JOIN sor_pgmsorteo ps ON ps.id = d.sor_pgmsorteo_id_c
							WHERE d.minuta > 0
							GROUP BY d.minuta, d.sor_producto_id_c, d.sor_pgmsorteo_id_c
							HAVING total_debe <> total_haber) dif;
	        SET 	RCode = 4, 
			RTxt = CONCAT("sor_publica_minutas. Error de cuadratura en contabilidad para el juego ", par_juego, " sorteo ", par_sorteo, " proceso ", par_proceso),
	 	        RId = 0,
			RSQLErrNo = 0,
			RSQLErrtxt = "";
			IF (@grupo_minuta = 'cc_p') THEN
					-- *****   CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!
					SET @RId = '0';
					CALL SP_ET_ConcurrenciaGET2('1','1','cas02_cc_parametros', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
					IF (@RId != '0') THEN
						SET @id_registro_concurrencia = NULL;
						SELECT COALESCE(a.description,0) INTO @id_registro_concurrencia
						FROM sor_aud_consolidacion a 
						WHERE a.id_pro_consolidacion = 
							(SELECT MAX(b.id_pro_consolidacion) 
							FROM sor_aud_consolidacion  b
							WHERE b.idjuego = par_juego AND b.nrosorteo = par_sorteo AND COALESCE(b.description,'') != ''
							)
						;
						IF (COALESCE(@id_registro_concurrencia,0) = par_juego*1000000+par_sorteo) THEN
							-- ***** LIBERO CONCURRENCIA !!!
							SET @id_concurrencia = @RId;
							SET @RId = '0';
							CALL SP_ET_ConcurrenciaDEL2(@id_concurrencia, 'N', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
							UPDATE sor_aud_consolidacion
							SET description = NULL
							WHERE description = @id_registro_auditoria
							;
						END IF;
					END IF;
					-- *****   FIN CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!	    
				END IF;			
		LEAVE fin;
	END IF;
	
	-- Si hay algo para publicar
	IF EXISTS(SELECT * FROM tmp_cas02_cc_cntpoc_det d
				WHERE d.minuta > 0 AND (d.importe + d.importea + d.importeb) > 0) THEN
				
		SET @etapa = @etapa + 1; -- 8
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - control de cuadratura - etapa: ', @etapa));
		DROP  
		TABLE IF EXISTS tmp_cas02_cc_cntpoc; 
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - drop tabla - etapa: ', @etapa));
		
		IF @grupo_minuta = 'ppag' THEN
			CREATE  
			TABLE tmp_cas02_cc_cntpoc
				SELECT t.minuta, 
					CONCAT(m.min_desc, IF(ps.id IS NULL, '', CONCAT(' Sorteo ', LPAD(ps.nrosorteo, 6, ' ')))) AS nombrecab, 
					CONCAT(m.min_desc, IF(ps.id IS NULL, '', CONCAT(' Sorteo ', LPAD(ps.nrosorteo, 6, ' '), ' ')), 
								t.debehaber, ' ', LPAD(t.orden, 3, ' '), LPAD(t.impcbl, 8, ' ')) AS nombredet, 
					t.orden, t.impcbl, 
				
					IF(t.debehaber = 'D', t.importe + importea + t.importeb, 0) AS debe, 
					IF(t.debehaber = 'H', t.importe + importea + t.importeb, 0) AS haber,
					CONCAT(COALESCE(CASE @grupo_minuta WHEN 'ppag' THEN t.fecha_pago ELSE ps.fecha END,				
							CURDATE()), '_', COALESCE(p.id_juego, 'sj'), '_', 
							COALESCE(ps.nrosorteo, 'ss'), '_', t.minuta, '_', RIGHT(par_proceso,10)) AS idcab,
					CONCAT(COALESCE(CASE @grupo_minuta WHEN 'ppag' THEN t.fecha_pago ELSE ps.fecha END, 
							CURDATE()), '_', COALESCE(p.id_juego, 'sj'), '_', 
							COALESCE(ps.nrosorteo, 'ss'), '_', t.minuta, '_', 
							t.debehaber, '_', t.orden, '_', RIGHT(par_proceso,10)) AS iddet,
					t.sor_producto_id_c, t.sor_pgmsorteo_id_c, t.fecha_pago
						FROM tmp_cas02_cc_cntpoc_det t
							JOIN sor_rec_ctaspcia_minutas m ON m.sor_producto_id_c = t.sor_producto_id_c AND m.min_id = t.minuta
							LEFT JOIN sor_producto p ON p.id = t.sor_producto_id_c
							LEFT JOIN sor_pgmsorteo ps ON ps.id = t.sor_pgmsorteo_id_c
						ORDER BY COALESCE(p.id_as400, 0), COALESCE(ps.nrosorteo, 0), t.fecha_pago, t.minuta, t.debehaber, t.orden, t.impcbl;
		ELSE
			CREATE  
			TABLE tmp_cas02_cc_cntpoc
				SELECT t.minuta, 
					CONCAT(m.min_desc, IF(ps.id IS NULL, '', CONCAT(' Sorteo ', LPAD(ps.nrosorteo, 6, ' ')))) AS nombrecab, 
					CONCAT(m.min_desc, IF(ps.id IS NULL, '', CONCAT(' Sorteo ', LPAD(ps.nrosorteo, 6, ' '), ' ')), 
								t.debehaber, ' ', LPAD(t.orden, 3, ' '), LPAD(t.impcbl, 8, ' ')) AS nombredet, 
					t.orden, t.impcbl, 
					IF(t.debehaber = 'D', t.importe + importea + t.importeb, 0) AS debe, 
					IF(t.debehaber = 'H', t.importe + importea + t.importeb, 0) AS haber,
					CONCAT(COALESCE( ps.fecha,				
							CURDATE()), '_', COALESCE(p.id_juego, 'sj'), '_', 
							COALESCE(ps.nrosorteo, 'ss'), '_', t.minuta, '_', RIGHT(par_proceso,10)) AS idcab,
					CONCAT(COALESCE( ps.fecha, 
							CURDATE()), '_', COALESCE(p.id_juego, 'sj'), '_', 
							COALESCE(ps.nrosorteo, 'ss'), '_', t.minuta, '_', 
							t.debehaber, '_', t.orden, '_', RIGHT(par_proceso,10)) AS iddet,
					t.sor_producto_id_c, t.sor_pgmsorteo_id_c
						FROM tmp_cas02_cc_cntpoc_det t
							JOIN sor_rec_ctaspcia_minutas m ON m.sor_producto_id_c = t.sor_producto_id_c AND m.min_id = t.minuta
							LEFT JOIN sor_producto p ON p.id = t.sor_producto_id_c
							LEFT JOIN sor_pgmsorteo ps ON ps.id = t.sor_pgmsorteo_id_c
						ORDER BY COALESCE(p.id_as400, 0), COALESCE(ps.nrosorteo, 0), t.minuta, t.debehaber, t.orden, t.impcbl;
		END IF;
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - create tabla - etapa: ', @etapa, ' - grupo de minuta:', @grupo_minuta));
		
		SET @trans = 1, @ahora = NOW();
		-- Borro los datos que hubiera del sorteo
		SET @etapa = @etapa + 1; -- 9
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - elimino registros del sorteo - etapa: ', @etapa));
		IF @grupo_minuta = 'ppag' THEN
		    SET @proceso = CONCAT(par_proceso, '%');
		ELSE
		    SET @proceso = par_proceso;
		END IF; 
		START TRANSACTION;
		DELETE r
			FROM cas02_cc_cntpoc_cab_cas02_cc_cntpoc_det_c r
				JOIN cas02_cc_cntpoc_cab c ON c.id = r.cas02_cc_cntpoc_cab_cas02_cc_cntpoc_detcas02_cc_cntpoc_cab_ida
			WHERE c.cab_identprc LIKE @proceso;
		DELETE r
			FROM cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cab_c r
				JOIN cas02_cc_cntpoc_cab c ON c.id = r.cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb
			WHERE c.cab_identprc LIKE @proceso;
		DELETE d 
			FROM cas02_cc_cntpoc_det d
				JOIN cas02_cc_cntpoc_cab c ON c.id = d.cas02_cc_cntpoc_cab_id_c
			WHERE c.cab_identprc LIKE @proceso;
		DELETE c
			FROM cas02_cc_cntpoc_cab c 
			WHERE c.cab_identprc LIKE @proceso;
		DELETE c
			FROM cas02_cc_cntpoc_grp c 
			WHERE c.grp_identprc LIKE @proceso;
			
		-- Incorporo grupo
		SET @etapa = @etapa + 1; -- 10
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto grupo - etapa: ', @etapa));
		
		IF @grupo_minuta = 'ppag' THEN
			INSERT INTO cas02_cc_cntpoc_grp
				SELECT CONCAT(par_proceso,'_',t.fecha_pago, '_', t.minuta), 
						CONCAT('Premios Pagados - Fecha ', t.fecha_pago) AS `name`, 
						@ahora, @ahora, usuario, usuario, NULL, 0, usuario,
						'P' AS grp_estado, 
						t.fecha_pago AS grp_feccon,         -- fecha contabilizacion 
						CURDATE(),  -- fecha proceso
						-- si es premios_pagados/prescriptos/ctacte agencia no corresponde SORTEO, ctacte provincia/qnl extra si! 
						MAX(IF(@grupo_minuta IN ('cc_p', 'remi', 'arem', 'dise','ppre'), t.sor_producto_id_c, '')) AS sor_producto_id_c, 
						-- si es premios_pagados/prescriptos/ctacte agencia no corresponde SORTEO, ctacte provincia/qnl extra si! 
						MAX(IF(@grupo_minuta IN ('cc_p', 'remi', 'arem','ppre'), t.sor_pgmsorteo_id_c, '')) AS sor_pgmsorteo_id_c, 
						CONCAT(par_proceso,'_',t.fecha_pago, '_', t.minuta)
				       FROM tmp_cas02_cc_cntpoc t
							LEFT JOIN sor_producto p ON p.id = t.sor_producto_id_c
							LEFT JOIN sor_pgmsorteo ps ON ps.id = t.sor_pgmsorteo_id_c
				       WHERE t.minuta > 0 AND t.debe + t.haber <> 0
				       GROUP BY t.minuta, t.fecha_pago;		
		ELSE
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto grupo - @grupo_minuta: ', @grupo_minuta));
			INSERT INTO cas02_cc_cntpoc_grp 
				SELECT par_proceso, 
						CASE @grupo_minuta 
							WHEN 'dise' THEN CONCAT('Distribuciones - Entregas a la red comercial hasta el ', DATE_FORMAT(CURDATE(), '%d/%m/%Y'))
							WHEN 'remi' THEN CONCAT('Distribuciones - Compra de Producto - Comprobante ', par_proceso)
							WHEN 'arem' THEN CONCAT('Distribuciones - Devolución de Producto - Comprobante ', par_proceso)
							WHEN 'cc_a' THEN CONCAT('CtaCte Agencia - proceso ', RIGHT(par_proceso,10))  -- cuenta corriente agencias
							WHEN 'dcha' THEN CONCAT('Doble Chance - proceso ', RIGHT(par_proceso,10))  -- doble chance
							-- WHEN 'ppre' THEN 'Premios Prescriptos ' 				    -- premios prescriptos
							WHEN 'ppre' THEN CONCAT('Premios Prescriptos - ',   			    -- premios prescriptos
										IF(p.id  IS NULL, '', CONCAT(' Juego ', p.name)),
										IF(ps.id IS NULL, '', CONCAT(' Sorteo ', LPAD(ps.nrosorteo, 6, ' '))))
							WHEN 'cc_p' THEN CONCAT('CtaCte Provincia - ',   			    -- cuenta corriente provincia
										IF(p.id  IS NULL, '', CONCAT(' Juego ', p.name)),
										IF(ps.id IS NULL, '', CONCAT(' Sorteo ', LPAD(ps.nrosorteo, 6, ' '))))
							ELSE par_proceso
						END AS `name`, 
						@ahora, @ahora, usuario, usuario, NULL, 0, usuario,
					       'P' AS grp_estado, 
					       
					       -- fecha contabilizacion
					       -- ps.fecha AS grp_feccon, se pone la del dia, pero aqui hay que mejorarlo. vale la fecha en la minuta
					       CURDATE() AS grp_feccon,
					       CURDATE(),  -- fecha proceso
					       -- si es premios_pagados/prescriptos/ctacte agencia no corresponde SORTEO, ctacte provincia/qnl extra si! 
					       MAX(IF(@grupo_minuta IN ('cc_p', 'remi', 'arem', 'dise','ppre', 'dcha'), t.sor_producto_id_c, '')) AS sor_producto_id_c, 
					       -- si es premios_pagados/prescriptos/ctacte agencia no corresponde SORTEO, ctacte provincia/qnl extra si! 
					       MAX(IF(@grupo_minuta IN ('cc_p', 'remi', 'arem','ppre', 'dcha'), t.sor_pgmsorteo_id_c, '')) AS sor_pgmsorteo_id_c, 
					       par_proceso
				       FROM tmp_cas02_cc_cntpoc t
							LEFT JOIN sor_producto p ON p.id = t.sor_producto_id_c
							LEFT JOIN sor_pgmsorteo ps ON ps.id = t.sor_pgmsorteo_id_c
				       -- WHERE t.minuta > 0 AND t.debe + t.haber <> 0
				       -- Filtro: si grupo minuta es 'cc_p', se seleccionan los registros donde minuta es mayor que 0. Modificado 2023-12-05
				       WHERE
					    (
						(@grupo_minuta = 'cc_p' AND t.minuta > 0) OR
						(@grupo_minuta <> 'cc_p' AND t.minuta > 0 AND t.debe + t.haber <> 0)
					    )
				       ;
		END IF;	       
			      
		-- Incorporo cabecera
		SET @etapa = @etapa + 1; -- 11
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto cabecera - etapa: ', @etapa));
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto cas02_cc_cntpoc_cab - @grupo_minuta: ', @grupo_minuta));
		
		IF @grupo_minuta = 'ppag' THEN
			INSERT INTO cas02_cc_cntpoc_cab 
				SELECT t.idcab, t.nombrecab, @ahora, @ahora, usuario, usuario, NULL, 0, usuario,
				       t.sor_pgmsorteo_id_c, t.sor_producto_id_c, SUM(t.debe), 'P', t.fecha_pago, t.minuta, CONCAT(par_proceso,'_',t.fecha_pago, '_', t.minuta), NULL
				FROM tmp_cas02_cc_cntpoc t
				WHERE t.minuta > 0 AND t.debe + t.haber <> 0
				GROUP BY t.idcab, t.nombrecab, t.sor_pgmsorteo_id_c, t.sor_producto_id_c, t.minuta, t.fecha_pago
			;
			/*
			INSERT INTO cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cab_c(id,date_modified,deleted, cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida, cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb)
				SELECT UUID(), NOW(), 0, grp.cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida, cab.cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb
				FROM
				(
					SELECT DISTINCT CONCAT(par_proceso,'_',t.fecha_pago, '_', t.minuta) AS cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida
					FROM tmp_cas02_cc_cntpoc t
							LEFT JOIN sor_producto p ON p.id = t.sor_producto_id_c
							LEFT JOIN sor_pgmsorteo ps ON ps.id = t.sor_pgmsorteo_id_c
					WHERE t.minuta > 0 AND t.debe + t.haber <> 0
					GROUP BY t.minuta, t.fecha_pago
				) grp,
				(
					SELECT DISTINCT t.idcab AS cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb
					FROM tmp_cas02_cc_cntpoc t
					WHERE t.minuta > 0 AND t.debe + t.haber <> 0
				) cab
			;
			*/
			
			INSERT INTO cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cab_c 
				(id,date_modified,deleted,cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida, 
								cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb)
				SELECT DISTINCT UUID(), NOW(), 0, CONCAT(par_proceso,'_',t.fecha_pago, '_', t.minuta), t.idcab 
				FROM tmp_cas02_cc_cntpoc t
				WHERE t.minuta > 0 AND t.debe + t.haber <> 0;
			
		ELSE
			INSERT INTO cas02_cc_cntpoc_cab 
				SELECT t.idcab, t.nombrecab, @ahora, @ahora, usuario, usuario, NULL, 0, usuario,
				       t.sor_pgmsorteo_id_c, t.sor_producto_id_c, SUM(t.debe), 'P', CURDATE(), t.minuta, par_proceso, NULL
				FROM tmp_cas02_cc_cntpoc t
				-- WHERE t.minuta > 0 AND t.debe + t.haber <> 0
				-- Filtro: si grupo minuta es 'cc_p', se seleccionan los registros donde minuta es mayor que 0. Modificado 2023-12-05
				WHERE
				(
					(@grupo_minuta = 'cc_p' AND t.minuta > 0) OR
					(@grupo_minuta <> 'cc_p' AND t.minuta > 0 AND t.debe + t.haber <> 0)
				)
				GROUP BY t.idcab, t.nombrecab, t.sor_pgmsorteo_id_c, t.sor_producto_id_c, t.minuta
			;
			INSERT INTO cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cab_c(id,date_modified,deleted, cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida, cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb)
				SELECT UUID(), NOW(), 0, grp.cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida, cab.cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb
				FROM
				(
					SELECT DISTINCT par_proceso AS cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_grp_ida
					FROM tmp_cas02_cc_cntpoc t
					LEFT JOIN sor_producto p ON p.id = t.sor_producto_id_c
					LEFT JOIN sor_pgmsorteo ps ON ps.id = t.sor_pgmsorteo_id_c
					WHERE
					(
						(@grupo_minuta = 'cc_p' AND t.minuta > 0) OR
						(@grupo_minuta <> 'cc_p' AND t.minuta > 0 AND t.debe + t.haber <> 0)
					)
				) grp,
				(
					SELECT DISTINCT t.idcab AS cas02_cc_cntpoc_grp_cas02_cc_cntpoc_cabcas02_cc_cntpoc_cab_idb
					FROM tmp_cas02_cc_cntpoc t
					WHERE
					(
						(@grupo_minuta = 'cc_p' AND t.minuta > 0) OR
						(@grupo_minuta <> 'cc_p' AND t.minuta > 0 AND t.debe + t.haber <> 0)
					)
				) cab
			;				
		END IF;
	       
		-- Incorporo detalle
		SET @etapa = @etapa + 1; -- 12
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto detalle - etapa: ', @etapa));
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto cas02_cc_cntpoc_det - @grupo_minuta: ', @grupo_minuta));		
		INSERT INTO cas02_cc_cntpoc_det 
			SELECT t.iddet, t.nombredet, @ahora, @ahora, usuario, usuario, NULL, 0, usuario,
			       t.impcbl, t.debe, t.haber, t.orden, t.idcab
			       FROM tmp_cas02_cc_cntpoc t
			       -- WHERE t.minuta > 0 AND t.debe + t.haber <> 0
			       -- Filtro: si grupo minuta es 'cc_p', se seleccionan los registros donde minuta es mayor que 0. Modificado 2023-12-05
				WHERE
				    (
					(@grupo_minuta = 'cc_p' AND t.minuta > 0) OR
					(@grupo_minuta <> 'cc_p' AND t.minuta > 0 AND t.debe + t.haber <> 0)
				    )
			       ;
			       
		       
		-- Incorporo relacion
		SET @etapa = @etapa + 1; -- 12
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto relacion - etapa: ', @etapa));
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - inserto cas02_cc_cntpoc_cab_cas02_cc_cntpoc_det_c - @grupo_minuta: ', @grupo_minuta));	
		INSERT INTO cas02_cc_cntpoc_cab_cas02_cc_cntpoc_det_c 
			SELECT t.iddet, @ahora, 0, t.idcab, t.iddet
			       FROM tmp_cas02_cc_cntpoc t
			       -- WHERE t.minuta > 0 AND t.debe + t.haber <> 0
			       -- Filtro: si grupo minuta es 'cc_p', se seleccionan los registros donde minuta es mayor que 0. Modificado 2023-12-05
			       WHERE
				    (
					(@grupo_minuta = 'cc_p' AND t.minuta > 0) OR
					(@grupo_minuta <> 'cc_p' AND t.minuta > 0 AND t.debe + t.haber <> 0)
				    )
			       ;
		/*
		-- Si grupo minuta es 'cc_p', eliminamos los registros en tabla de relacion y los detalles. Agregado 2023-12-05
		-- de esta forma las minutas con total de debe y haber > 0 no visualizarán las ref contables que hayan quedado en 0 (cero)	
		if @grupo_minuta = 'cc_p' then 
			
			DELETE r
			FROM cas02_cc_cntpoc_cab_cas02_cc_cntpoc_det_c r
			INNER JOIN cas02_cc_cntpoc_cab c ON c.id = r.cas02_cc_cntpoc_cab_cas02_cc_cntpoc_detcas02_cc_cntpoc_cab_ida AND r.deleted = 0
			INNER JOIN cas02_cc_cntpoc_det d ON d.id = r.cas02_cc_cntpoc_cab_cas02_cc_cntpoc_detcas02_cc_cntpoc_det_idb AND d.deleted = 0
			WHERE 
				c.cab_nrominuta > 0
				AND d.det_debe + d.det_haber = 0     
				AND c.cab_importe <> 0
				AND c.cab_identprc = par_proceso
				AND c.deleted = 0
			;
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - fin delete cas02_cc_cntpoc_cab_cas02_cc_cntpoc_det_cc - @grupo_minuta: ', @grupo_minuta, 'par_proceso: ', par_proceso));	
			
			DELETE d
			FROM cas02_cc_cntpoc_det d
			INNER JOIN cas02_cc_cntpoc_cab c ON c.id = d.cas02_cc_cntpoc_cab_id_c
			WHERE 
				c.cab_nrominuta > 0
				AND d.det_debe + d.det_haber = 0     
				AND c.cab_importe <> 0
				AND c.cab_identprc = par_proceso
				AND c.deleted = 0
			;
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - fin delete cas02_cc_cntpoc_det - @grupo_minuta: ', @grupo_minuta, 'par_proceso: ', par_proceso));	
		end if;
		
		-- -------------------------------------------
		*/
		COMMIT;		
		SET @trans = 0;
	ELSE
		-- Incorporo relacion
		SET @etapa =  12; -- 13
		INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - NO HAY MINUTAS PARA CONTABILIZAR - etapa: ', @etapa));
	END IF;
	SET @etapa = @etapa + 1; -- 14
	
	IF (@grupo_minuta = 'cc_p') THEN
		-- *****   CONTROL DE CONCURRENCIA particular: SI EXISTE UNA EN CURSO LA LIBERO !!
		SET @RId = '0';
		CALL SP_ET_ConcurrenciaGET2('1','1','cas02_cc_parametros', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		IF (@RId != '0') THEN
			SET @id_registro_concurrencia = NULL;
			SELECT COALESCE(a.description,0) INTO @id_registro_concurrencia
			FROM sor_aud_consolidacion a 
			WHERE a.id_pro_consolidacion = 
				(SELECT MAX(b.id_pro_consolidacion) 
				FROM sor_aud_consolidacion  b
				WHERE b.idjuego = par_juego AND b.nrosorteo = par_sorteo AND COALESCE(b.description,'') != ''
				)
			;
			IF (COALESCE(@id_registro_concurrencia,0) = par_juego*1000000+par_sorteo) THEN
				-- ***** LIBERO CONCURRENCIA !!!
				SET @id_concurrencia = @RId;
				SET @RId = '0';
				CALL SP_ET_ConcurrenciaDEL2(@id_concurrencia, 'N', @RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
				UPDATE sor_aud_consolidacion
				SET description = NULL
				WHERE description = @id_registro_auditoria
				;
			END IF;
		END IF;
		-- *****   FIN CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!	    
	END IF;
	
	-- *****   CONTROL DE CONCURRENCIA: SI EXISTE UNA EN CURSO LA LIBERO !!
	IF (COALESCE(@concurr_gral,'0') != '0') THEN
		CALL SP_ET_ConcurrenciaDEL2(@concurr_gral, 'N',@RCode,@RTxt,@RId,@RSQLErrNo,@RSQLErrtxt);
		IF (@RCode != 1) THEN
			INSERT INTO kk_auditoria2 (nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - Concurrencia general - FIN ERROR CALL SP_ET_ConcurrenciaDEL2 - id_concurrencia: ',@concurr_gral,' - Rtxt: ', @RTxt));
		ELSE
			INSERT INTO kk_auditoria2 (nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - Concurrencia general - FIN OK CALL SP_ET_ConcurrenciaDEL2 - id_concurrencia: ',@concurr_gral));
		END IF;
	END IF;
	SET 	RCode = 0,
		RTxt = "OK",
		RId = 0,
		RSQLErrNo = 0,
		RSQLErrtxt = "";
	SET @etapa = @etapa + 1; -- 14
	INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - fin OK - etapa: ', @etapa));
			INSERT INTO kk_auditoria2(nombre) VALUES(CONCAT(NOW(), ' sor_publica_minutas - FIN OK - juego ', par_juego, 
							' sorteo ', par_sorteo, ' proceso ', par_proceso, ' - etapa: ' , @etapa));
    END$$

DELIMITER ;