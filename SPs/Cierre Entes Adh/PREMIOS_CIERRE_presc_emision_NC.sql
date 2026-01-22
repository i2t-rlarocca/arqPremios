DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_CIERRE_presc_emision_NC`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_CIERRE_presc_emision_NC`(
		IN param_lst_str_ids VARCHAR(4096),		
		IN param_id_proceso  INT,
		IN param_opc         INT,
		IN param_idUsuario   VARCHAR(36),
		OUT msgerr           VARCHAR(4096))
thisSP:
BEGIN
    DECLARE usuario VARCHAR(36);
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	GET DIAGNOSTICS CONDITION 1
	@code = RETURNED_SQLSTATE,
	@msg = MESSAGE_TEXT,
	@errno = MYSQL_ERRNO,
	@base = SCHEMA_NAME,
	@tabla = TABLE_NAME;
	SET msgerr = CONCAT('Problema de ejecución de SP PREMIOS_CIERRE_presc_emision_NC', @msg);
END;
	
    SET usuario = param_idUsuario;
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - INI - param_lst_str_ids: ', param_lst_str_ids, ' - param_id_proceso: ', param_id_proceso, ' param_opc: ', param_opc));
	SET @param_lst_str_ids = param_lst_str_ids;
	SET @id_proceso  = param_id_proceso;
	SET @param_opc         = param_opc;
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
		         ;")
	;
	PREPARE sentenciasql3 FROM @query3;
	-- SELECT @query3;
	EXECUTE sentenciasql3
	;
	SET @lstNroSorteos = REPLACE(CASE WHEN RIGHT(TRIM(@lstNroSorteos),1) = '-' THEN LEFT(@lstNroSorteos, LENGTH(@lstNroSorteos) -1) ELSE @lstNroSorteos END,',',''); 
	-- Creo tabla temporal tmp_emisiones
	DROP TEMPORARY TABLE IF EXISTS tmp_emisiones; 
	CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emisiones (
		id_juego INT(11),
		NAME	CHAR(255),
		estado  CHAR(128)
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
	-- estado 2 = premio cerrado
	SET @query = "";
	SET @query = CONCAT(@query,
		"INSERT INTO tmp_emisiones (id_juego,NAME,estado)
		SELECT 	distinct
			ps.idjuego as id_juego,
			CASE 
			WHEN ps.sor_emision_cerrada !=1 THEN TRIM(ps.name)
			WHEN re.estado != '2'          THEN TRIM(re.name) 
			ELSE 'NO IDENTIFICADO'
			END AS resumen_emision,		
			CASE 
			WHEN ps.sor_emision_cerrada !=1 THEN TRIM(ps.sor_emision_cerrada)
			WHEN re.estado != '2'          THEN TRIM(ld.valor)
			ELSE 'SIN ESTADO'
			END AS estado
		FROM pre_resumen_emision re 
		INNER JOIN sor_pgmsorteo ps ON ps.id = re.sor_pgmsorteo_id_c
		INNER JOIN tbl_listas_desplegables ld ON ld.codigo = re.estado
		WHERE re.sor_pgmsorteo_id_c IN (", @param_lst_str_ids ,")
		AND (ps.sor_emision_cerrada != 1 AND  re.estado != '2')
		AND re.pre_canal_de_pago_id_c in ('O','T');");
	PREPARE sentenciasql FROM @query;
	-- SELECT @query;
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - query: ', @query));
	EXECUTE sentenciasql;	
	
	-- CURSOR PARA VALIDAR LAS EMISIONES SELECCIONADAS.
	-- Declaros las variables para el cursor cur_emisiones
BEGIN
	DECLARE v_mensaje        VARCHAR(4096) DEFAULT '';
	DECLARE v_done           INT DEFAULT FALSE;	
	DECLARE v_id_juego   INT(11);
	DECLARE v_emision_id_juego   INT(11);
	DECLARE v_emision_name   CHAR(255);
	DECLARE v_emision_estado CHAR(128);    
	
	-- Cursor para recorrer los resultados de la tabla temporal tmp_emisiones
	DECLARE cur_emisiones CURSOR FOR 
	SELECT DISTINCT id_juego, NAME, estado FROM tmp_emisiones;
	
	-- Manejador para finalizar el bucle
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;	
    
	-- Abro cursor cur_emisiones
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - Abro cursor cur_emisiones'));
	OPEN cur_emisiones;	
	read_loop: LOOP
		-- Fetch del cursor cur_emisiones
		FETCH NEXT FROM cur_emisiones INTO v_emision_id_juego, v_emision_name, v_emision_estado;		
		
		-- Salgo del bucle si no hay más filas
		IF v_done THEN			
			LEAVE read_loop;
		END IF;
		SET v_id_juego = v_emision_id_juego;
		
		-- Concatenar fila al mensaje
		SET v_mensaje = CONCAT(v_mensaje, ' EMISION: ', v_emision_name, ' - ESTADO: \t', v_emision_estado, '\n');		
	END LOOP read_loop;	
	CLOSE cur_emisiones; -- Cierro cursor cur_emisiones
	
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - Cierro cursor cur_emisiones'));
 
	IF (LENGTH(TRIM(v_mensaje)) > 0) THEN 
		SET v_mensaje = CONCAT('EXISTEN RESUMENES DE EMISION Y/O EMISIONES QUE NO ESTAN CERRADOS VERIFIQUE:\n', v_mensaje);
				
		SET msgerr = v_mensaje;		
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - salida msgerr: ', msgerr));
		
		LEAVE thisSP;	
	ELSE 		
		IF (v_id_juego IN (4,13,30)) THEN
			-- Llamar al procedimiento almacenado para GENERAR NOTAS DE CREDITOS
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - call CC_NC_Prescripcion_CtaCte_Provincia @param_lst_str_ids: ', @param_lst_str_ids));
			
			CALL sor_inserta_auditoria(@id_proceso,@prod_id, -1,84,usuario,81,85,CONCAT("NCs Pcias - INICIO - Producto: ", @prod_name, " - Sorteos: ", @lstNroSorteos));
			CALL CC_NC_Prescripcion_CtaCte_Provincia(@param_lst_str_ids, usuario, @rcode, @rtxt, @id, @rsqlerrno, @rsqlerrtxt);		
			
			-- Verificar el resultado de la llamada al stored
			IF (@rcode <> 1) THEN
				CALL sor_inserta_auditoria(@id_proceso,@prod_id, -1,88,usuario,84,0,CONCAT("NCs Pcias - FIN FALLIDO- Producto: ", @prod_name, " - Sorteos: ", @lstNroSorteos));
				INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - Problema al generar las NCs por prescripción - detalle: (', @rcode, ') - ', @rtxt));
				SET msgerr = "Atención: surgió un problema al generar las notas de creditos";
				LEAVE thisSP;		
			END IF;	
			CALL sor_inserta_auditoria(@id_proceso,@prod_id, -1,85,usuario,84,86,CONCAT("NCs Pcias - FIN CORRECTO - Producto: ", @prod_name, " - Sorteos: ", @lstNroSorteos));
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - mensaje: ', @rtxt));					
		END IF;
	END IF;
END;
	INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - marco los resumenes de emision como: CONTABILIZANDO PRESCRIPCION estado 5'));
	-- MARCAR LOS RESUMENES DE EMISION COMO CONTABILIZANDO PRESCRIPCION (estado=5)
	SET @query2 = "";
	SET @query2 = CONCAT(@query2,"
		UPDATE pre_resumen_emision re			
		SET 	re.estado = '5', -- Contabilizando presc.			
			re.date_modified = @hoy
		WHERE re.sor_pgmsorteo_id_c IN (", @param_lst_str_ids ,");");
	PREPARE sentenciasql2 FROM @query2;
	-- select @query2;
	EXECUTE sentenciasql2;
	-- Creo tabla temporal tmp_emisiones_minutas
	DROP TEMPORARY TABLE IF EXISTS tmp_emisiones_minutas; 
	CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emisiones_minutas (
		id                 CHAR(36),
		juego              INT NOT NULL,
		sorteo             INT NOT NULL,
		idenprc		   VARCHAR(255),
		estado		   INT 
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
	-- estado 4 = nc generada
	-- SET @idenprc = CONCAT('ppre_', (v_juego * 1000000 + v_sorteo));
	
	SET @query3 = "";
	SET @query3 = CONCAT(@query3,
		"INSERT INTO tmp_emisiones_minutas (id, juego, sorteo, idenprc)
		SELECT         
		    distinct ps.id, ps.idjuego, ps.nrosorteo, CONCAT('ppre_', (ps.idjuego * 1000000 + ps.nrosorteo)) as idenprc
		FROM pre_resumen_emision re 
		INNER JOIN sor_pgmsorteo ps ON ps.id = re.sor_pgmsorteo_id_c
		INNER JOIN tbl_listas_desplegables ld ON ld.codigo = re.estado
		WHERE re.sor_pgmsorteo_id_c IN (", @param_lst_str_ids ,")
		AND (ps.sor_emision_cerrada = 1 and  re.estado = '5')  
		AND re.pre_canal_de_pago_id_c = 'O';");
	PREPARE sentenciasql3 FROM @query3;
	-- SELECT @query3;
	EXECUTE sentenciasql3;  
	  
       INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - query3: ', @query3));
	
	-- CURSOR PARA LAS MINUTAS.
	-- Declaros las variables para el cursor cur_emisiones_min
BEGIN	
	DECLARE v_done_min     INT DEFAULT FALSE;
	DECLARE v_id_pgm       CHAR(36);	
	DECLARE v_juego        INT(255);
	DECLARE v_sorteo       INT(255);  
	DECLARE v_idenprc      VARCHAR(255);  
	
	-- Cursor para recorrer los resultados de la tabla temporal tmp_emisiones_minutas
	DECLARE cur_emisiones_minutas CURSOR FOR 
	SELECT id, juego, sorteo, idenprc FROM tmp_emisiones_minutas;
	
	-- Manejador para finalizar el bucle
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done_min = TRUE;	
	
	CALL sor_inserta_auditoria(@id_proceso,@prod_id, -1,86,usuario,85,87,CONCAT("Minutas - INICIO - Producto: ", @prod_name, " - Sorteos: ", @lstNroSorteos));
	
	OPEN cur_emisiones_minutas;    
	read_loop_min: LOOP
		-- Fetch del cursor cur_emisiones_minutas
		FETCH NEXT FROM cur_emisiones_minutas INTO v_id_pgm, v_juego, v_sorteo, v_idenprc;        
			
		-- Salgo del bucle si no hay más filas
		IF v_done_min THEN            
		    LEAVE read_loop_min;
		END IF;
		
		INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' kk- PREMIOS_CIERRE_presc_emision_NC - call sor_publica_minutas v_juego: ', v_juego, ' v_sorteo: ', v_sorteo, ' v_idenprc: ', v_idenprc));
		
		CALL sor_publica_minutas(v_juego, v_sorteo, usuario, v_idenprc, @rcode, @rtxt, @id, @rsqlerrno, @rsqlerrtxt);
		
		-- SET @rcode = 0;
		IF (@rcode != 0) THEN	
			CALL sor_inserta_auditoria(@id_proceso,v_juego, v_sorteo,88,usuario,86,0,CONCAT("Minutas - FIN FALLIDO - Producto: ", @prod_name, " - Sorteo: ", v_sorteo));
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - Error durante generacion de minutas - provincia - error: ', @rcode, ' - mensaje: ', @rtxt));	
			SET msgerr = CONCAT("Problema al generar las minutas: ", @rtxt);		
			LEAVE thisSP;
		ELSE 
			CALL sor_inserta_auditoria(@id_proceso,v_juego, v_sorteo,87,usuario,86,89,CONCAT("Minutas - FIN CORRECTO - Producto: ", @prod_name, " - Sorteo: ", v_sorteo));
			
			INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - marco los resumenes de emision como: PRESCRIPCIÓN CONTABILIZADA estado 6'));
			
			-- MARCAR LA EMISION COMO PRESCRIPCIÓN CONTABILIZADA
			START TRANSACTION;
				INSERT INTO tmp_estado_proc_cierre_emis_presc(id_sorteo,nro_sorteo,juego,estadoproc,cod_estado,idproceso,date_entered,date_modified)
				SELECT pg.id, pg.nrosorteo, pg.idjuego, CONCAT('OK-',v_idenprc), 0, @id_proceso,NOW(),NOW() 
				FROM sor_pgmsorteo pg 
				WHERE pg.id = v_id_pgm;
				UPDATE tmp_emisiones_minutas
				SET estado = 1
				WHERE juego = v_juego AND  sorteo = v_sorteo 
				;
				UPDATE pre_resumen_emision re			
				SET 	re.estado = '6', -- PRESC CONTAB.				
					re.date_modified = NOW()
				WHERE re.sor_pgmsorteo_id_c = v_id_pgm
				;
				UPDATE sor_pgmsorteo			
				SET sor_presc_contabilizada = 1, -- PRESC CONTAB.
				    sor_fechor_presc_contabilizada = NOW()
				WHERE id = v_id_pgm
				;			
			COMMIT;
		END IF;
	
	END LOOP read_loop_min;	
	CLOSE cur_emisiones_minutas;
END;
CALL sor_inserta_auditoria(@id_proceso,@prod_id, -1,89,usuario,87,0,CONCAT("Cierre y Presc. de Emisión - FIN CORRECTO  - Producto: ", @prod_name, " - Sorteos: ", @lstNroSorteos));
INSERT INTO kk_auditoria2(nombre) VALUES (CONCAT(NOW(), ' - PREMIOS_CIERRE_presc_emision_NC - FIN'));
SET msgerr = CONCAT("OK. Finalizaci&oacute;n correcta del proceso de Generaci&oacute;n de NCs y minutas por prescripci&oacute;n - Producto: ", @prod_name, " - Sorteos: ", @lstNroSorteos,' - Proceso: ',@id_proceso);
END$$

DELIMITER ;