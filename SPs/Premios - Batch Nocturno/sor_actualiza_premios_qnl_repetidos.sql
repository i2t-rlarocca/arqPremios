DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `sor_actualiza_premios_qnl_repetidos`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `sor_actualiza_premios_qnl_repetidos`(
		OUT RCode INT,
		OUT RTxt LONGTEXT,
		OUT RId	VARCHAR(36),
		OUT RSQLErrNo INT,
		OUT RSQLErrtxt VARCHAR(2048)
    )
    COMMENT 'Actualiza premios de QNL repetidos'
BEGIN
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN	
		GET DIAGNOSTICS CONDITION 1
		 	@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
			@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
		
		SET @code = 1, @errno = 1, @msg = 'Error...', @base='', @tabla='';
		
		IF (@trans = 1) THEN
			ROLLBACK;
		END IF;
		SET 	RCode = @code,
			RTxt = "Error",
			RId = 0,
			RSQLErrNo = @errno,
			RSQLErrtxt = CONCAT(@msg, ' en tabla ', @tabla, ' de ', @base, '  - llegó a etapa: ', @etapa) ;
			
	END;
	SET @etapa = 0,
	    @trans = 0,
	    @ahora = NOW();
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - INICIO - etapa: " , @etapa));
	-- 1. Recupero parametros
	SET @etapa = @etapa + 1; -- 1
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - recupero parametros - etapa: " , @etapa));
	SELECT p.par_cant_min_rep, p.par_imp_min_rep, NOW()
			INTO @par_cant_min_rep, @par_imp_min_rep, @ahora
		FROM pre_parametros p;
	-- SELECT @par_cant_min_rep, @par_imp_min_rep, @ahora;
	-- 2. localizo los sorteos a recalcular, ultimos 3 días solo quinielas
	SET @etapa = @etapa + 1; -- 2
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - creo tmp_pgmsorteo_a_proc_qnl - etapa: " , @etapa));
	DROP TEMPORARY TABLE IF EXISTS tmp_pgmsorteo_a_proc_qnl;
	CREATE TEMPORARY TABLE tmp_pgmsorteo_a_proc_qnl
		SELECT DISTINCT ps.id, ps.fecha, DAYOFWEEK(ps.fecha) AS dow
			FROM sor_pgmsorteo ps 
				JOIN sor_producto p ON p.id = ps.sor_producto_id_c
				LEFT JOIN pre_premios_qnl_repetidos pr ON pr.sor_pgmsorteo_id_c = ps.id
			WHERE ps.fecha >= DATE_SUB(CURDATE(), INTERVAL 3 DAY) 
				AND p.sor_categoria_juego_id_c = 5 -- solo las quinielas!
				AND pr.id IS NULL
			;
	ALTER TABLE `tmp_pgmsorteo_a_proc_qnl` ADD INDEX `KEY` (`id`); 	
	-- SELECT * FROM tmp_pgmsorteo_a_proc_qnl;
	-- 3. localizo los sorteos a recalcular, ultimos 3 días solo quinielas
	SET @etapa = @etapa + 1; -- 3
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - creo tmp_premios_rep_a_insertar - premios_menores - etapa: " , @etapa));
	DROP TEMPORARY TABLE IF EXISTS tmp_premios_rep_a_insertar;
	CREATE TEMPORARY TABLE tmp_premios_rep_a_insertar
	SELECT t.id, p.account_id_c, p.age_permiso_id_c, p.pre_impbruto, t.fecha, t.dow, COUNT(*) AS cant_pre, SUM(p.pre_impbruto) AS imp_total
		FROM tmp_pgmsorteo_a_proc_qnl t
			JOIN pre_premios_menores p ON p.sor_pgmsorteo_id_c = t.id
		WHERE p.pre_impbruto >= @par_imp_min_rep
		GROUP BY t.id, p.account_id_c, p.age_permiso_id_c, p.pre_impbruto, t.fecha, t.dow
		HAVING cant_pre >= @par_cant_min_rep
		;
	-- 4. localizo los sorteos a recalcular, ultimos 3 días solo quinielas
	SET @etapa = @etapa + 1; -- 4
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - extiendo tmp_premios_rep_a_insertar - premios - etapa: " , @etapa));
	INSERT INTO tmp_premios_rep_a_insertar
	SELECT t.id, p.account_id_c, p.age_permiso_id_c, p.pre_impbruto, t.fecha, t.dow, COUNT(*) AS cant_pre, SUM(p.pre_impbruto) AS imp_total
		FROM tmp_pgmsorteo_a_proc_qnl t
			JOIN pre_premios p ON p.sor_pgmsorteo_id_c = t.id
		WHERE p.pre_impbruto >= @par_imp_min_rep
		GROUP BY t.id, p.account_id_c, p.age_permiso_id_c, p.pre_impbruto, t.fecha, t.dow
		HAVING cant_pre >= @par_cant_min_rep
		;
	-- 5. elimino premios de los sorteos a actualizar
	SET @etapa = @etapa + 1; -- 5
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - elimino premios de los sorteos a actualizar - etapa: " , @etapa));
	DELETE t
		FROM tmp_pgmsorteo_a_proc_qnl t
			JOIN pre_premios_qnl_repetidos pr ON pr.sor_pgmsorteo_id_c = t.id;
	SET @RowCount = ROW_COUNT();
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - elimino premios de los sorteos a actualizar - etapa: " , @etapa, " - eliminados:", @RowCount));
	-- 6. incorporo los premios
	SET @etapa = @etapa + 1; -- 6
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - incorporo los premios - etapa: " , @etapa));
	INSERT INTO pre_premios_qnl_repetidos
		    (id, NAME, date_entered, date_modified, modified_user_id, created_by, description, deleted, assigned_user_id,
			pql_fecha, pql_cantidad, pql_importe, pql_importe_unitario, pql_dow, sor_pgmsorteo_id_c, age_permiso_id_c, account_id_c)
	SELECT UUID(), 
		CONCAT(LPAD(p.numero_agente, 5, ' '), '/', LPAD(p.punto_venta_numero, 3, ' '), ' - ', RPAD(LEFT(TRIM(p.razon_social), 20), 20, ' '), ' (', LPAD(p.id_permiso,5,' '), ') - ', ps.name) AS pdv,
		@ahora, @ahora, 1, 1, NULL, 0, 1,
		 t.fecha, t.cant_pre, t.imp_total, t.pre_impbruto, t.dow, t.id, t.age_permiso_id_c, t.account_id_c
			FROM tmp_premios_rep_a_insertar t
				JOIN age_permiso p ON p.id = t.age_permiso_id_c
				JOIN sor_pgmsorteo ps ON ps.id = t.id
	;
	SET @RowCount = ROW_COUNT();
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), " sor_actualiza_premios_qnl_repetidos - incorporo los premios - etapa: " , @etapa, " - insertados:", @RowCount));
	SET 	RCode = 0,
		RTxt = "OK!",
		RId = 0,
		RSQLErrNo = 0,
		RSQLErrtxt = '';
	
    END$$

DELIMITER ;