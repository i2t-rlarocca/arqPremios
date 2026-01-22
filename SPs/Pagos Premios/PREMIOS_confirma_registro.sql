DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_confirma_registro`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_confirma_registro`(
	IN par_Canal_de_Pago CHAR(1), 
	IN par_Premios_ID_C CHAR(36)
    )
exterior:BEGIN
		DECLARE hecho BOOLEAN DEFAULT FALSE;
		DECLARE v_id_mensaje INTEGER DEFAULT 0;
		DECLARE v_mensaje VARCHAR(255) DEFAULT '';
		
		DECLARE v_codigo_autorizacion VARCHAR(10) DEFAULT '';
		
		DECLARE v_opp_estado_registracion VARCHAR(100) DEFAULT NULL;
		DECLARE v_tbl_Provincias_ID_C CHAR(36) DEFAULT '';
		DECLARE v_idJuego INT(11) DEFAULT 0;
		DECLARE v_nrosorteo INT(11) DEFAULT 0;
		DECLARE v_pre_secuencia INT(11) DEFAULT 0;
		DECLARE	v_pre_retenciones DECIMAL(11,2) DEFAULT 0;
		DECLARE v_pre_tipopremio CHAR(1) DEFAULT '0';
		DECLARE v_pre_Canal_de_Pago_ID_C CHAR(36) DEFAULT NULL;
		DECLARE v_pre_Orden_Pago_ID_C CHAR(36) DEFAULT NULL;
		DECLARE v_pre_Tipo_Comprobante_Juego_Canal_ID_C CHAR(36) DEFAULT NULL;
		DECLARE v_pre_Talonarios_ID_C CHAR(36) DEFAULT NULL;
		DECLARE v_pre_Tipo_Comprobante_ID_C  CHAR(36) DEFAULT NULL;
		DECLARE v_pre_Orden_Pago_Beneficiarios_ID_C   CHAR(36) DEFAULT NULL;
		DECLARE v_pre_Tipo_Comprobante VARCHAR(10) DEFAULT NULL;
		
		DECLARE v_fecha_pago, v_fecha_minima DATE DEFAULT NULL;
		DECLARE v_tipo_usuario VARCHAR(255) DEFAULT '';
		
		-- SET v_mensaje = 'Error durante operacion de actualizacion';		
		
		DECLARE var_brincosueldo INT DEFAULT 2; 	-- PREMIO SUELDO --> PARA PREMIO BRINCO SUELDO
		DECLARE var_brinco INT DEFAULT 13; 		-- BRINCO --> PARA TRATAMIENTO PREMIO ESTIMULO Y CUOTAS
		DECLARE var_totobingo INT DEFAULT 215;          -- TOTOBINGO --> PARA EXCLUSION DE LA IMP_GANANCIAS
		DECLARE var_telekino  INT DEFAULT 217;          -- TELEKINO --> PARA EXCLUSION DE LA IMP_GANANCIAS
		DECLARE var_maradona  INT DEFAULT 268;          -- J.CON MARADONA --> PARA EXCLUSION DE LA IMP_GANANCIAS
		
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN	
			GET DIAGNOSTICS CONDITION 1
				@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
				@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
		
			SET v_mensaje = CONCAT('Error durante operacion de actualizacion - etapa: ', @etapa, @subetapa, ' detalle: ', @msg, '(', @errno, ')'), v_id_mensaje = 9, hecho = FALSE;
			ROLLBACK;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa, ' - SUBETAPA ', @subetapa, ' - msg ', @msg, '-', @errno, ' - ROLLBACK');
			-- Retorno ok
			SELECT COALESCE(v_tbl_Provincias_ID_C, '') 	AS provincia, 
				COALESCE(v_idJuego,0) 			AS juego, 
				COALESCE(v_nroSorteo, 0) 		AS sorteo, 
				COALESCE(v_pre_secuencia, 0) 		AS secuencia, 
				hecho 					AS ejecOk, 
				v_id_mensaje 				AS idMsg, 
				v_mensaje 				AS msg, 
				COALESCE(v_codigo_autorizacion, '') 	AS codigoAutorizacion;
		END;
cuerpo:BEGIN
		-- PARA EL PROCESO DE GENERACION AUTOMATICA, LO NECESITABA MUDO, SIEMPRE ERA "A", LE MANDE "H" y convierto!!!
		/*
		-- RL: 2019-04-23: Cuando llamo desde Cambiar Canal también tengo que tener off
		set @par_Canal_de_Pago =
			case par_Canal_de_Pago 
				when 'H' then 'A'
				when 'X' then 'O'
				WHEN 'Y' THEN 'T'
				else par_Canal_de_Pago
			end
		;
		SET @echo =
			CASE par_Canal_de_Pago 
				WHEN 'H' THEN 'off'
				WHEN 'X' THEN 'off'
				WHEN 'Y' THEN 'off'
				ELSE 'on'
			END
		;		
		*/ 
		
		IF par_Canal_de_Pago = 'H' THEN
			SET @par_Canal_de_Pago = 'A', @echo = 'off';
		ELSE
			SET @par_Canal_de_Pago = par_Canal_de_Pago, @echo = 'on';
		END IF;
		
		-- FIN RL: 2019-04-23: Cuando llamo desde Cambiar Canal también tengo que tener off
		
		SET @etapa = 0, @subetapa = 0; -- 0
		SET @subetapa = @subetapa + 1; -- 1
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		-- El canal de pago "debe" existir
		SET @subetapa = @subetapa + 1; -- 2
		SELECT id INTO v_pre_Canal_de_Pago_ID_C
			FROM pre_canal_de_pago cp 
			WHERE cp.codigo_canal = @par_Canal_de_Pago AND cp.deleted = 0;
		IF v_pre_Canal_de_Pago_ID_C IS NULL THEN
			SET v_mensaje = 'No existe el canal de pago indicado', v_id_mensaje = 2, hecho = FALSE;
			LEAVE cuerpo;
		END IF;
		
		/* Obtener datos del ticket */
		/* Control datos Estados y Fechas, datos Beneficiario, Cobrador, Forma Pago */
		/* Actualizar Estado Premio */
		/* Obtener Número de Habilitación de Pago del Premio */
		/* TST: Configuración datos prueba - BORRAR!!! */
		-- Recupero datos básicos
		SET @subetapa = @subetapa + 1; -- 2
		SELECT COALESCE(pp.pre_nroautcobro, ''), 
			opp.id, COALESCE(opp.opp_estado_registracion, 'P'), 
			ps.idjuego, ps.nrosorteo, pp.pre_secuencia, pp.tbl_provincias_id_c, 
			pp.pre_ret_ley20630 + pp.pre_ret_ley23351 AS pre_retenciones, pp.pre_tipopremio, -- Agrega AE
			tjc.id, t.id, tc.id, tc.name, opb.id,
			-- Para validar FECHA DE PAGO si el usuario de carga es de tipo PROVINCIA
			opp.opp_fecha_pago, UPPER(COALESCE(tu.name, 'PROVINCIA')), opp.opp_fpago, -- por defecto, que limite
			opb.name 								  -- ADEN - 2024-11-28 - no puede ser null el name del beneficiario principal!
			
		INTO v_codigo_autorizacion, 
				v_pre_Orden_Pago_ID_C, v_opp_estado_registracion, 
				v_idJuego, v_nrosorteo, v_pre_secuencia, v_tbl_Provincias_ID_C,
				v_pre_retenciones, v_pre_tipopremio,
				v_pre_Tipo_Comprobante_Juego_Canal_ID_C, v_pre_Talonarios_ID_C, v_pre_Tipo_Comprobante_ID_C, v_pre_Tipo_Comprobante, 
				v_pre_Orden_Pago_Beneficiarios_ID_C,
				v_fecha_pago, v_tipo_usuario, @opp_fpago,
				@beneficiario							  -- ADEN - 2024-11-28 - no puede ser null el name del beneficiario principal!
			
		FROM pre_premios pp
		INNER JOIN sor_pgmsorteo ps ON ps.id = pp.sor_pgmsorteo_id_c AND ps.deleted = 0
		LEFT JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pp.id AND COALESCE(opp.pre_premios_cuotas_id_c, '') = '' AND opp.deleted = 0
		LEFT JOIN pre_orden_pago_beneficiarios opb ON opb.`pre_orden_pago_id_c` = opp.`id` AND opb.`opb_tipobeneficiario` = 'B' AND opb.`deleted` = 0
		LEFT JOIN pre_tipo_comprobante_juego_canal tjc ON tjc.pre_canal_de_pago_id_c = v_pre_Canal_de_Pago_ID_C 
								AND tjc.sor_producto_id_c = ps.sor_producto_id_c
								AND tjc.tcj_tipopremio = pp.pre_tipopremio
								AND tjc.deleted = 0
		LEFT JOIN pre_tipo_comprobante tc ON tc.id = tjc.pre_tipo_comprobante_id_c AND tc.deleted = 0
		LEFT JOIN pre_talonarios t ON t.id = tc.pre_talonarios_id_c AND t.deleted = 0
		LEFT JOIN users u ON u.id = opp.assigned_user_id AND u.deleted = 0
		LEFT JOIN users_cstm uc ON uc.id_c = u.id
		LEFT JOIN i2t01_tipos_usuarios tu ON tu.id = uc.i2t01_tipos_usuarios_id_c AND tu.deleted = 0
		WHERE pp.id = par_Premios_ID_C
		;
		-- INSERT INTO kk VALUES(CONCAT('idj', v_idJuego, ' vidj ', var_brinco, ' tpr ', v_pre_tipopremio, ' vtpr ', var_brincosueldo));
		-- *** CONTROL DE CONCURRENCIA: Si hay un vuelco de premios pagados activo cancelo la confirmacion
		SET @RId = '0';
		CALL SP_ET_ConcurrenciaGET2('1','1','cas02_cc_rec_premiosPagados_ctrl',
			@RCode,
			@RTxt,
			@RId,
			@RSQLErrNo,
			@RSQLErrtxt
		);
		IF (@RId != '0') THEN
			SET v_mensaje = CONCAT('CONCURRENCIA: Existe un proceso de vuelco de premios pagados activo. Espere unos instantes y vuelva a intentar. )'), v_id_mensaje = 9, hecho = FALSE;
			-- Retorno ok
			SELECT COALESCE(v_tbl_Provincias_ID_C, '') 	AS provincia, 
				COALESCE(v_idJuego,0) 			AS juego, 
				COALESCE(v_nroSorteo, 0) 		AS sorteo, 
				COALESCE(v_pre_secuencia, 0) 		AS secuencia, 
				hecho 					AS ejecOk, 
				v_id_mensaje 				AS idMsg, 
				v_mensaje 				AS msg, 
				COALESCE(v_codigo_autorizacion, '') 	AS codigoAutorizacion
			;			
		END IF;
		-- *** FIN CONTROL DE CONCURRENCIA: Si hay un vuelco de premios pagados activo cancelo la confirmacion
					
		-- El regitro de liquidacion debe existir...
		SET @subetapa = @subetapa + 1; -- 3
		IF v_opp_estado_registracion IS NULL THEN 
			SET v_mensaje = 'Se solicita confirmación de una liquidación no ingresada', v_id_mensaje = 3, v_codigo_autorizacion = '', hecho = FALSE;
		ELSE
			-- La parametrización de talonario x canal/juego debe existir...
			IF v_pre_Tipo_Comprobante_Juego_Canal_ID_C IS NULL THEN 
				SET v_mensaje = CONCAT('No se definió TALONARIO para el canal ', @par_Canal_de_Pago, ' código de juego ', v_idJuego, ' tipo de premio "', v_pre_tipopremio, '" billete ', v_pre_secuencia), v_codigo_autorizacion = '', v_id_mensaje = 4, hecho = FALSE;
			ELSE
				-- El talonario debe existir...
				IF v_pre_Talonarios_ID_C IS NULL THEN 
					SET v_mensaje = CONCAT('No se encuentra el TALONARIO para el canal ', v_codigo_autorizacion = '', @par_Canal_de_Pago, ' código de juego ', v_idJuego), v_id_mensaje = 5, hecho = FALSE;
				ELSE 
					-- El talonario debe existir...
					IF v_pre_Tipo_Comprobante_ID_C IS NULL THEN 
						SET v_mensaje = CONCAT('No se encuentra el TIPO DE COMPROBANTE del talonario', v_pre_Talonarios_ID_C ), v_codigo_autorizacion = '', v_id_mensaje = 6, hecho = FALSE;
					ELSE 
						-- El BENEFICIARIO debe existir...
						IF v_pre_Orden_Pago_Beneficiarios_ID_C IS NULL OR @beneficiario IS NULL THEN -- ADEN - 2024-11-28 -- se agrega el beneficiario
							SET v_mensaje = 'No se encuentra el beneficiario ', v_codigo_autorizacion = '', v_id_mensaje = 7, hecho = FALSE;
						ELSE
						    IF @par_Canal_de_Pago = 'O' AND v_fecha_pago IS NULL THEN
							SET v_mensaje = 'No se informó fecha de pago ', v_codigo_autorizacion = '', v_id_mensaje = 8, hecho = FALSE;
						    END IF;
						END IF;
					END IF;
				END IF;
			END IF;
		END IF;
		
                -- Si no hay error, y el usuario que ingresa es de tipo PROVINCIA, valido la fecha de pago!
		SET @subetapa = @subetapa + 1; -- 4                
		IF v_id_mensaje = 0 AND v_tipo_usuario = 'PROVINCIA' THEN
		        SET v_fecha_minima = '2100-12-31' ;
			SELECT v.vto_fecha_desde INTO v_fecha_minima
				FROM imp_vencimientos v
					JOIN (SELECT v1.imp_impuesto, MIN(v1.vto_fecha_cierre_adm) AS max_ca
						FROM imp_vencimientos v1
							WHERE v1.imp_impuesto = 'SIC'
								AND v1.vto_fecha_cierre_adm >= CURDATE()
								AND v1.deleted = 0
							GROUP BY v1.imp_impuesto) va ON va.imp_impuesto = v.imp_impuesto AND va.max_ca = v.vto_fecha_cierre_adm
						WHERE v.deleted = 0
						;
			IF v_fecha_pago < v_fecha_minima THEN
				SET v_mensaje = CONCAT('No se pueden registrar premios pagados antes del ', DATE_FORMAT(v_fecha_minima, '%d/%m/%Y')), v_codigo_autorizacion = '', v_id_mensaje = 8, hecho = FALSE;
			END IF;
		END IF;
		IF v_id_mensaje > 0 THEN
			LEAVE cuerpo;
		END IF;
		-- COMIENZA TRANSACCION 
		SET @subetapa = @subetapa + 1; -- 5
		
		START TRANSACTION;
		SET @subetapa = @subetapa + 1; -- 6
		
		-- Proceso numerador (si corresponde)
		IF v_opp_estado_registracion = 'P' THEN 	-- Si estaba pendiente, tengo que calcular el proximo número
			-- ****************************************** INICIO DE TRANSACCION *****************************************
			SET @etapa = 1; -- 1
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa, ' ingreso por calcular nro' );
-- INI ANTES
			INSERT INTO kk 
				SELECT CONCAT(NOW(), ' - confirma premios-id: ',opp.id, 
							' - estregistrobenef: ', p.pre_estregistrobenef , 
							' - estado_emision_ddjj: ', opp.opp_estado_emision_ddjj, 
							' - par_Canal_de_Pago: ', @par_Canal_de_Pago,
							' - talonario ', v_pre_Talonarios_ID_C
						) AS nombre
						FROM pre_orden_pago opp 
							JOIN pre_premios p ON p.id = opp.pre_premios_id_c AND p.deleted = 0
						WHERE opp.id = v_pre_Orden_Pago_ID_C;
-- FIN ANTES
			-- Actualizo numerador
			-- Actualizo el talonario 
			UPDATE pre_talonarios t 
					SET t.proximo_numero = t.proximo_numero + 1 
				WHERE t.id = v_pre_Talonarios_ID_C AND t.deleted = 0;
			
			-- Actualizo la operacion
			SET @etapa = 2; -- 2
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa, ' - talonario:', v_pre_Talonarios_ID_C);
			UPDATE pre_orden_pago opp 
				JOIN pre_talonarios t ON t.id = v_pre_Talonarios_ID_C AND t.deleted = 0
				JOIN pre_premios p ON p.id = opp.pre_premios_id_c AND p.deleted = 0
				JOIN accounts_cstm ac ON ac.id_c = p.`account_id_c` 
					SET 	-- opp.name = concat(t.letra, '-', Lpad(t.punto_venta, 4, '0'), '-', Lpad(t.proximo_numero, 8, 0)),
						opp.name = CONCAT(TRIM(v_pre_Tipo_Comprobante), '-', LPAD(t.proximo_numero, 8, 0)),
						opp.pre_tipo_comprobante_id_c =  v_pre_Tipo_Comprobante_ID_C,
						opp.pre_canal_de_pago_id_c = @par_Canal_de_Pago,
						opp.pre_talonarios_id_c = v_pre_Talonarios_ID_C,
						opp.opp_letra = t.letra,
						opp.opp_punto_venta = t.punto_venta,
						opp.opp_numero = t.proximo_numero,
						opp.opp_fecha_comprobante = CURDATE(),
						opp.opp_estado_registracion = 'D',
						-- Si el canal es OTRAS PROVINCIAS, ajusto para que sea a las 11:00am
						opp.opp_fecha_pago = IF(@par_Canal_de_Pago = 'O', 
						                        DATE_FORMAT(opp.opp_fecha_pago, '%Y-%m-%d 11:00:00'), 
						                        -- Agrega AE - 2019-02-22 para que fije fecha de pago cuando es ctacte
									IF(@par_Canal_de_Pago = 'T' AND opp.opp_fpago = 4, CURDATE(), opp.opp_fecha_pago)), 
						-- opp_estado_actualizacion_fpag = N -> NO APLICA / P -> PENDIENTE / C -> COMPLETA
						-- opp.opp_estado_actualizacion_fpag = if(@par_Canal_de_Pago = 'T', 'P', 'C'), 
						-- El estado de actualizacion de fecha de pago lo fija es SP DE PREMIOS PAGADOS PARA AGENCIA 
						-- y el proceso de RECUPERO DE PREMIOS
						-- CORREGIDO AE 2019-01-31 si es TESORERIA fuerzo pendiente
						-- CORREGIDO AE 2019-02-22 si es TESORERIA y CTACTE fuerzo completa
						opp.opp_estado_actualizacion_fpag = IF(@par_Canal_de_Pago = 'O' OR (@par_Canal_de_Pago = 'T' AND opp.opp_fpago = 4),
												'C', 
												IF(@par_Canal_de_Pago = 'T', 'P', opp.opp_estado_actualizacion_fpag)), 
						-- si canal es OTRAS PROVINCIAS 
						-- 	forma de pago '1' = EFECTIVO, si no lo que se eligio!
						opp.opp_fpago = IF(@par_Canal_de_Pago = 'O', '1', opp.opp_fpago),
						-- 	agente la provincia, si no lo que se eligio!
						opp.`opp_agente_pago` = IF(@par_Canal_de_Pago = 'O', ac.`numero_agente_c`, opp.`opp_agente_pago`), 
						-- 	subagente la provincia, si no lo que se eligio!
						opp.`opp_subagente_pago` = IF(@par_Canal_de_Pago = 'O', ac.`numero_subagente_c`, opp.`opp_subagente_pago` ),
						-- 	cuenta la provincia, si no lo que se eligio!
						opp.`account_id_c` = IF(@par_Canal_de_Pago = 'O', p.`account_id_c`, opp.`account_id_c`),
						-- opp_estado_emision_ddjj = N -> NO APLICA / P -> PENDIENTE / C -> COMPLETA
						opp.opp_estado_emision_ddjj = IF(@par_Canal_de_Pago = 'O' OR p.pre_tiporegben <> 'C', 'N', 'P'),
						-- estado documentacion
						opp.opp_estado_recep_doc_uif = IF(@par_Canal_de_Pago = 'A' AND p.pre_tiporegben = 'C', 'pte', opp.opp_estado_recep_doc_uif)
					WHERE opp.id = v_pre_Orden_Pago_ID_C;
/* -- no va más - aden - 2025-08-11						
			-- SI ES UN BRINCO SUELDO Y EL BENEFICIARIO ES PERSONA FISICA Y NO SE INDICÓ TIPO/NUMERO DE DOCUMENTO, SE FUERZA LO QUE SURGE DEL CUIT!
			IF v_idJuego = var_brinco AND v_pre_tipopremio = var_brincosueldo THEN
				UPDATE pre_orden_pago_beneficiarios opb
					JOIN pre_orden_pago opp ON opp.id = opb.`pre_orden_pago_id_c` AND opp.`deleted` = 0
						SET 	
							-- si el nro de doc es cero, fuerzo la parte del cuit!
							opb.`opb_doc_nro` = IF(COALESCE(opb.`opb_doc_nro`, 0) = 0, RIGHT(LEFT(opb.`opb_cuit`, 10), 8), opb.`opb_doc_nro`),
							-- si el tipo de doc es cero, fuerzo '3'-> DNI
							opb.`opb_tdoc` = IF(COALESCE(opb.`opb_tdoc`, 0) = 0, '3', opb.`opb_tdoc`)
						WHERE opp.id = v_pre_Orden_Pago_ID_C
							AND opb.`opb_tipo` = 'F';
			END IF;
*/			
-- INI DESPUES
			SET @etapa = 3;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			INSERT INTO kk 
				SELECT CONCAT(NOW(), ' - confirma premios-id: ',opp.id, 
							' - estregistrobenef: ', p.pre_estregistrobenef , 
							' - estado_emision_ddjj: ', opp.opp_estado_emision_ddjj, 
							' - par_Canal_de_Pago: ', @par_Canal_de_Pago,
							' - opp_fpago: ', @opp_fpago
						) AS nombre
						FROM pre_orden_pago opp 
							JOIN pre_premios p ON p.id = opp.pre_premios_id_c AND p.deleted = 0
						WHERE opp.id = v_pre_Orden_Pago_ID_C;
-- FIN DESPUES
			SET @etapa = 4;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			UPDATE pre_premios pp
				SET 
				    pp.`pre_estregistrobenef` = 'C',
					
				    -- Si se trata de un premio otra provincia, se da por pagado!
				    pp.pre_estadopago = CASE 
							-- WHEN @par_Canal_de_Pago = 'O' 	THEN 'A'
							WHEN ((@par_Canal_de_Pago = 'O') 
								  OR (@par_Canal_de_Pago = 'T' AND @opp_fpago = 4)) THEN 'A'
					                							    ELSE pp.pre_estadopago
							END,
				    -- Si pago otra provincia, no debe informarse a UIF
				    pp.pre_estinfuif =	CASE 
								WHEN @par_Canal_de_Pago = 'O' 	THEN 'N'
												ELSE pp.pre_estinfuif
							END
				WHERE pp.id = par_Premios_ID_C;
		ELSE
			SET @etapa = 1;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios (ACTUALIZACION)-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			-- Actualizo la operacion
			SET @etapa = 2;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios (ACTUALIZACION)-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			UPDATE pre_orden_pago opp 
				JOIN pre_premios p ON p.id = opp.pre_premios_id_c AND p.deleted = 0
				JOIN accounts_cstm ac ON ac.id_c = p.`account_id_c` 
					SET 	
						-- El estado de actualizacion de fecha de pago lo fija es SP DE PREMIOS PAGADOS PARA AGENCIA 
						-- y el proceso de RECUPERO DE PREMIOS
						-- CORREGIDO AE 2019-01-31 si es TESORERIA fuerzo pendiente
						-- CORREGIDO AE 2019-02-22 si es TESORERIA y CTACTE fuerzo completa
						opp.opp_estado_actualizacion_fpag = IF(@par_Canal_de_Pago = 'O' OR (@par_Canal_de_Pago = 'T' AND opp.opp_fpago = 4),
												'C', 
												IF(@par_Canal_de_Pago = 'T', 'P', opp.opp_estado_actualizacion_fpag)), 
						-- opp_estado_actualizacion_fpag = N -> NO APLICA / P -> PENDIENTE / C -> COMPLETA
						-- Si el canal es OTRAS PROVINCIAS, ajusto para que sea a las 11:00am
						opp.opp_fecha_pago = IF(@par_Canal_de_Pago = 'O', 
						                        DATE_FORMAT(opp.opp_fecha_pago, '%Y-%m-%d 11:00:00'), 
						                        -- Agrega AE - 2019-02-22 para que fije fecha de pago cuando es ctacte
									IF(@par_Canal_de_Pago = 'T' AND opp.opp_fpago = 4, CURDATE(), opp.opp_fecha_pago)), 
						-- si canal es OTRAS PROVINCIAS 
						-- 	forma de pago '1' = EFECTIVO, si no lo que se eligio!
						opp.opp_fpago = IF(@par_Canal_de_Pago = 'O', '1', opp.opp_fpago),
						-- 	agente la provincia, si no lo que se eligio!
						opp.`opp_agente_pago` = IF(@par_Canal_de_Pago = 'O', ac.`numero_agente_c`, opp.`opp_agente_pago`), 
						-- 	subagente la provincia, si no lo que se eligio!
						opp.`opp_subagente_pago` = IF(@par_Canal_de_Pago = 'O', ac.`numero_subagente_c`, opp.`opp_subagente_pago` ),
						-- 	cuenta la provincia, si no lo que se eligio!
						opp.`account_id_c` = IF(@par_Canal_de_Pago = 'O', p.`account_id_c`, opp.`account_id_c`),
						-- opp_estado_emision_ddjj = N -> NO APLICA / P -> PENDIENTE / C -> COMPLETA
						opp.opp_estado_emision_ddjj = IF(@par_Canal_de_Pago = 'O' OR p.pre_tiporegben <> 'C', 'N', 'P'),  
						-- estado documentacion
						opp.opp_estado_recep_doc_uif = IF(@par_Canal_de_Pago = 'A' AND p.pre_tiporegben = 'C', 'pte', opp.opp_estado_recep_doc_uif)
						/* SACAR COMENTARIO PARA FORZAR REENVIO AL AS/400
						, 
						-- Vuelvo a forzar el envio al as/400
						opp.estado_envio_as400 = 'P'
						*/
					WHERE opp.id = v_pre_Orden_Pago_ID_C;
					
			SET @etapa = 4;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios (ACTUALIZACION)-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			UPDATE pre_premios pp
				SET -- Si se trata de un premio otra provincia, se da por pagado!
				    pp.`pre_estregistrobenef` = 'C', -- AJUSTADO ADEN - 2025-08-11 - PARA LLEGAR ACÁ, TENIA QUE ESTAR EN "P" !!!!					
				    pp.pre_estadopago = CASE 
								WHEN ((@par_Canal_de_Pago = 'O') 
									OR (@par_Canal_de_Pago = 'T' AND @opp_fpago = 4)) 	
												THEN 'A'
												ELSE pp.pre_estadopago
							END,
				    -- Si pago otra provincia, no debe informarse a UIF
				    pp.pre_estinfuif =	CASE 
								WHEN @par_Canal_de_Pago = 'O' 	THEN 'N'
												ELSE pp.pre_estinfuif
							END
				WHERE pp.id = par_Premios_ID_C;
		END IF; -- si era provisorio!!!!
		SET @subetapa = @subetapa + 1; -- 7
		
		/* COMUN A INSERT Y UPDATE */
		-- SI ES UN BRINCO SUELDO Y EL BENEFICIARIO ES PERSONA FISICA Y NO SE INDICÓ TIPO/NUMERO DE DOCUMENTO, SE FUERZA LO QUE SURGE DEL CUIT!
		IF v_idJuego = var_brinco AND v_pre_tipopremio = var_brincosueldo THEN
			UPDATE pre_orden_pago_beneficiarios opb
				JOIN pre_orden_pago opp ON opp.id = opb.`pre_orden_pago_id_c` AND opp.`deleted` = 0
					SET 	
						-- si el nro de doc es cero, fuerzo la parte del cuit!
						opb.`opb_doc_nro` = IF(COALESCE(opb.`opb_doc_nro`, 0) = 0, RIGHT(LEFT(opb.`opb_cuit`, 10), 8), opb.`opb_doc_nro`),
						-- si el tipo de doc es cero, fuerzo '3'-> DNI
						opb.`opb_tdoc` = IF(COALESCE(opb.`opb_tdoc`, 0) = 0, '3', opb.`opb_tdoc`)
					WHERE opp.id = v_pre_Orden_Pago_ID_C
						AND opb.`opb_tipo` = 'F';
		END IF;
	        
		-- Inserto la persona (solo si no existe a partir del beneficiario
		SET @etapa = 5;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		INSERT INTO uif_persona
			    (`id`,`name`,`date_entered`,`date_modified`,`modified_user_id`,`created_by`,`description`,`deleted`,`assigned_user_id`,
			     `per_tipo`,`per_cuit`,`per_nombre`,`per_doc_tipo`,`per_doc_nro`,`per_domi`,`per_sexo`,`per_fechanac`,`per_estado_civil`,
			     `per_email`,`per_telefono`,`per_pep`,`per_cargo`,`per_apemat`,`tbl_localidades_id_c`,`tbl_provincias_id_c`,
			     `per_riesgo_actual`,`per_riesgo_fecultcalc`,`tbl_nacionalidades_id_c`,`per_riesgo_prom_act`,`per_riesgo_prom_max`,
			     `per_riesgo_feccalcmax`,`tbl_ocupaciones_id_c`,`per_domi_calle`,`per_domi_nro`,`per_domi_piso`,`per_domi_dpto`,
			     `per_telefono_carac`,`per_telefono_numero`,`per_tipo_soc`,`per_actividad`,`per_fescritura`,`per_validada_afip`,
			     `per_snombre`,`per_apellido`,`tbl_paises_id_c`,`per_tipo_documental_afip`,`per_nombre2`)
		SELECT opb.opb_cuit AS id, 
		       UPPER(CONCAT(	TRIM(CONCAT(TRIM(COALESCE(opb.opb_apellido,'')), ' ', TRIM(COALESCE(opb.opb_apemat,'')))),  ', ', 
				TRIM(CONCAT(TRIM(COALESCE(opb.opb_nombre,'')), ' ', TRIM(COALESCE(opb.opb_snombre,'')))))) AS NAME, 
			NOW() AS date_entered,
			NOW() AS date_modified,
			1 AS modified_user_id,
			1 AS created_by,
			UPPER(CONCAT(	TRIM(CONCAT(TRIM(COALESCE(opb.opb_apellido,'')), ' ', TRIM(COALESCE(opb.opb_apemat,'')))),  ', ', 
				TRIM(CONCAT(TRIM(COALESCE(opb.opb_nombre,'')), ' ', TRIM(COALESCE(opb.opb_snombre,'')))))) AS description,
			0 AS deleted,
			1 AS assigned_user_id,
			opb.opb_tipo AS per_tipo,
			opb.opb_cuit AS per_cuit,
			UPPER(COALESCE(opb.opb_nombre, '')) AS per_nombre,
			COALESCE(opb.opb_tdoc, '') AS per_doc_tipo,
			COALESCE(opb.opb_doc_nro, 0) AS per_doc_nro,
			UPPER(COALESCE(opb.opb_domi, '')) AS per_domi,
			COALESCE(opb.opb_sexo, '') AS per_sexo,
			opb.opb_fechanac AS per_fechanac, -- tiene null si no está informada
			COALESCE(opb.opb_estado_civil, '') AS per_estado_civil,
			COALESCE(opb.opb_email, '') AS per_email,
			TRIM(CONCAT(TRIM(COALESCE(opb.opb_telefono_carac,'')), ' ', TRIM(COALESCE(opb.opb_telefono_numero,'')))) AS per_telefono,
			COALESCE(opb.opb_pep, 'N') AS per_pep,
			UPPER(COALESCE(opb.opb_cargo, '')) AS per_cargo,
			COALESCE(opb.opb_apemat, '') AS per_apemat,
			COALESCE(opb.tbl_localidades_id_c, '') AS tbl_localidades_id_c,
			COALESCE(opb.tbl_provincias_id_c, '') AS tbl_provincias_id_c,
			0 AS per_riesgo_actual,
			NULL AS per_riesgo_fecultcalc,
			COALESCE(opb.tbl_nacionalidades_id_c, '') AS tbl_nacionalidades_id_c,
			0 AS per_riesgo_prom_act,
			0 AS per_riesgo_prom_max,
			NULL AS per_riesgo_feccalcmax,
			COALESCE(opb.tbl_ocupaciones_id_c, '') AS tbl_ocupaciones_id_c,
			UPPER(TRIM(COALESCE(opb.opb_domi_calle, ''))) AS per_domi_calle,
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_nro, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_nro)
			END AS per_domi_nro, 
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_piso, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_piso)
			END AS per_domi_piso,
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_dpto, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_dpto)
			END AS per_domi_dpto,
			COALESCE(opb.opb_telefono_carac, '') AS per_telefono_carac,
			COALESCE(opb.opb_telefono_numero, '') AS per_telefono_numero,
			COALESCE(opb.opb_tipo_soc, '') AS per_tipo_soc,
			'' AS per_actividad,
			NULL AS per_fescritura,
			NULL AS per_validada_afip,
			UPPER(TRIM(COALESCE(opb.opb_snombre, ''))) AS per_snombre,
			UPPER(TRIM(COALESCE(opb.opb_apellido, ''))) AS per_apellido,
			COALESCE(opb.tbl_paises_id_c, '') AS tbl_paises_id_c,
			COALESCE(opb.tipo_documental_afip, '') AS per_tipo_documental_afip,
			UPPER(TRIM(COALESCE(opb.opb_snombre, ''))) AS per_nombre2
			
			FROM pre_premios pp
				JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pp.id AND COALESCE(opp.pre_premios_cuotas_id_c, '') = '' AND opp.deleted = 0
				JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'B' AND opb.deleted = 0
				JOIN pre_tipo_comprobante tc ON tc.id = opp.pre_tipo_comprobante_id_c AND tc.deleted = 0
				LEFT JOIN uif_persona p ON p.per_cuit = opb.opb_cuit AND p.deleted = 0
				
			WHERE pp.id = par_Premios_ID_C AND p.id IS NULL 
				-- AND opb_cuit IS NOT NULL -- Agrego RL: evitar problemas de name = null
				AND LEFT(COALESCE(opb_cuit, '5'), 1) <> '5' -- Verifica que NO SEA NULL y que no sea tipo PAIS
				AND opb.opb_cuit NOT IN (SELECT cuit FROM cuit_paises_v)
			;		
					
		-- Inserto la persona (solo si no existe a partir del cobrador
		SET @etapa = 6;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		INSERT INTO uif_persona
			    (`id`,`name`,`date_entered`,`date_modified`,`modified_user_id`,`created_by`,`description`,`deleted`,`assigned_user_id`,
			     `per_tipo`,`per_cuit`,`per_nombre`,`per_doc_tipo`,`per_doc_nro`,`per_domi`,`per_sexo`,`per_fechanac`,`per_estado_civil`,
			     `per_email`,`per_telefono`,`per_pep`,`per_cargo`,`per_apemat`,`tbl_localidades_id_c`,`tbl_provincias_id_c`,
			     `per_riesgo_actual`,`per_riesgo_fecultcalc`,`tbl_nacionalidades_id_c`,`per_riesgo_prom_act`,`per_riesgo_prom_max`,
			     `per_riesgo_feccalcmax`,`tbl_ocupaciones_id_c`,`per_domi_calle`,`per_domi_nro`,`per_domi_piso`,`per_domi_dpto`,
			     `per_telefono_carac`,`per_telefono_numero`,`per_tipo_soc`,`per_actividad`,`per_fescritura`,`per_validada_afip`,
			     `per_snombre`,`per_apellido`,`tbl_paises_id_c`,`per_tipo_documental_afip`,`per_nombre2`)
		SELECT opb.opb_cuit AS id, 
		       CONCAT(	TRIM(CONCAT(TRIM(COALESCE(opb.opb_apellido,'')), ' ', TRIM(COALESCE(opb.opb_apemat,'')))),  ', ', 
				TRIM(CONCAT(TRIM(COALESCE(opb.opb_nombre,'')), ' ', TRIM(COALESCE(opb.opb_snombre,''))))) AS NAME, 
			NOW() AS date_entered,
			NOW() AS date_modified,
			1 AS modified_user_id,
			1 AS created_by,
			CONCAT(	TRIM(CONCAT(TRIM(COALESCE(opb.opb_apellido,'')), ' ', TRIM(COALESCE(opb.opb_apemat,'')))),  ', ', 
				TRIM(CONCAT(TRIM(COALESCE(opb.opb_nombre,'')), ' ', TRIM(COALESCE(opb.opb_snombre,''))))) AS description,
			0 AS deleted,
			1 AS assigned_user_id,
			opb.opb_tipo AS per_tipo,
			opb_cuit AS per_cuit,
			COALESCE(opb.opb_nombre, '') AS per_nombre,
			COALESCE(opb.opb_tdoc, '') AS per_doc_tipo,
			COALESCE(opb.opb_doc_nro, 0) AS per_doc_nro,
			COALESCE(opb.opb_domi, '') AS per_domi,
			COALESCE(opb.opb_sexo, '') AS per_sexo,
			opb.opb_fechanac AS per_fechanac, -- tiene null si no está informada
			COALESCE(opb.opb_estado_civil, '') AS per_estado_civil,
			COALESCE(opb.opb_email, '') AS per_email,
			TRIM(CONCAT(TRIM(COALESCE(opb.opb_telefono_carac,'')), ' ', TRIM(COALESCE(opb.opb_telefono_numero,'')))) AS per_telefono,
			COALESCE(opb.opb_pep, 'N') AS per_pep,
			COALESCE(opb.opb_cargo, '') AS per_cargo,
			COALESCE(opb.opb_apemat, '') AS per_apemat,
			COALESCE(opb.tbl_localidades_id_c, '') AS tbl_localidades_id_c,
			COALESCE(opb.tbl_provincias_id_c, '') AS tbl_provincias_id_c,
			0 AS per_riesgo_actual,
			NULL AS per_riesgo_fecultcalc,
			COALESCE(opb.tbl_nacionalidades_id_c, '') AS tbl_nacionalidades_id_c,
			0 AS per_riesgo_prom_act,
			0 AS per_riesgo_prom_max,
			NULL AS per_riesgo_feccalcmax,
			COALESCE(opb.tbl_ocupaciones_id_c, '') AS tbl_ocupaciones_id_c,
			TRIM(COALESCE(opb.opb_domi_calle, '')) AS per_domi_calle,
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_nro, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_nro)
			END AS per_domi_nro, 
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_piso, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_piso)
			END AS per_domi_piso,
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_dpto, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_dpto)
			END AS per_domi_dpto,
			COALESCE(opb.opb_telefono_carac, '') AS per_telefono_carac,
			COALESCE(opb.opb_telefono_numero, '') AS per_telefono_numero,
			COALESCE(opb.opb_tipo_soc, '') AS per_tipo_soc,
			'' AS per_actividad,
			NULL AS per_fescritura,
			NULL AS per_validada_afip,
			TRIM(COALESCE(opb.opb_snombre, '')) AS per_snombre,
			TRIM(COALESCE(opb.opb_apellido, '')) AS per_apellido,
			COALESCE(opb.tbl_paises_id_c, '') AS tbl_paises_id_c,
			COALESCE(opb.tipo_documental_afip, '') AS per_tipo_documental_afip,
			TRIM(COALESCE(opb.opb_snombre, '')) AS per_nombre2
			
			FROM pre_premios pp
				JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pp.id AND COALESCE(opp.pre_premios_cuotas_id_c, '') = '' AND opp.deleted = 0
				JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'C' AND opb.deleted = 0
				JOIN pre_tipo_comprobante tc ON tc.id = opp.pre_tipo_comprobante_id_c AND tc.deleted = 0
				LEFT JOIN uif_persona p ON p.per_cuit = opb.opb_cuit AND p.deleted = 0
			WHERE pp.id = par_Premios_ID_C AND p.id IS NULL
				-- AND opb_cuit IS NOT NULL -- Agrego RL: evitar problemas de name = null
				AND LEFT(COALESCE(opb_cuit, '5'), 1) <> '5' -- Verifica que NO SEA NULL y que no sea tipo PAIS
				AND opb.opb_cuit NOT IN (SELECT cuit FROM cuit_paises_v)
			;	
/* ADEN - 2024-06-24
	1) la tabla de retenciones está en el MODULO CONTABLE! no la actualizo mas!
	2) tambien elimino el fragmento relacionado a premio cuota (brinco) que ya no existe!!!
				
		-- Creo el registro de impuesto x ganancias, si y solo si tiene retencion (Y NO ES UN BINGO!)...
		
		IF v_pre_retenciones > 0 
		   -- AND v_idJuego NOT IN (var_totobingo, var_telekino, var_maradona) THEN
		   AND v_idJuego NOT IN (var_totobingo) THEN -- Solo el totobingo!
			SET @etapa = 71;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			DELETE FROM imp_ganancias WHERE id = par_Premios_ID_C;
			SET @etapa = 72;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			INSERT INTO imp_ganancias
				    (`id`, `name`, `date_entered`,`date_modified`,`modified_user_id`,`created_by`,`description`,`deleted`,`assigned_user_id`,
				     `gan_origen`,`gan_tipo_identif`,`gan_cuit`,`gan_tipcmp`,`gan_nrocmp`,`gan_feccmp`,`gan_impcmp`,`gan_impbase`,
				     `gan_fecret`,`gan_condgan`,`gan_impret`,`gan_alic`,`gan_codimp`,`gan_codreg`,`gan_esttrf`,`gan_razsoc`,
				     `gan_domi_calle`,`gan_domi_nro`,`gan_domi_piso`,`gan_domi_dpto`,`tbl_localidades_id_c`,`tbl_provincias_id_c`,
				     `gan_extranjero`, `gan_provincia_extranjera`,`gan_localidad_extranjera`,`gan_cpos_extranjero`,`tbl_paises_id_c`)
				     
				SELECT  opp.id, -- que coincida con la orden de pago
					opp.name, -- ver como lo armamos!
					opp.date_entered, -- que coincida con la orden de pago
					opp.date_modified, -- que coincida con la orden de pago
					opp.modified_user_id, -- que coincida con la orden de pago
					opp.created_by, -- que coincida con la orden de pago
					opp.description, -- que coincida con la orden de pago
					opp.deleted, -- que coincida con la orden de pago
					opp.assigned_user_id, -- que coincida con la orden de pago
					2 			 	AS gan_origen, -- es un premio
					opb.tipo_documental_afip 	AS gan_tipo_identif, -- 
					opb.opb_cuit 			AS gan_cuit, 
					LEFT(tc.name, 4) 		AS gan_tipcmp, 
--						CONCAT(opp.opp_letra, '-', LPAD(opp.opp_punto_venta, 4, '0'), '-', LPAD(opp.opp_numero, 8, 0)) 
					CONCAT(TRIM(tc.name), '-', LPAD(opp.opp_numero, 8, 0)) 
									AS gan_nrocmp,
					opp.opp_fecha_comprobante	AS gan_feccmp, -- fecha del comprobante de pago
						-- pp.pre_secuencia, 
						-- pp.tbl_provincias_id_c, 
					pp.pre_impbruto					AS gan_impcmp,
					pp.pre_impimponible				AS gan_impbase, 
					opp.opp_fecha_comprobante 			AS gan_fecret, -- fecha de retencion, coincide con la fecha del comprobante de pago
					'' 						AS gan_condgan, -- codigo de condicion ante ganancias - hay que ingresarlo
					pp.pre_ret_ley20630 + pp.pre_ret_ley23351 	AS gan_impret, 
					31 						AS gan_alic, -- por ahora FIJO (31%) - ver como calcularlo o de donde se saca!!!!
					'466' 						AS gan_codimp, -- FIJO, corresponde a premios
					'434'						AS gan_codreg, -- FIJO, corresponde a premios
					0						AS gan_esttrf, -- NO TRANSFERIDO
					CONCAT(TRIM(opb.opb_apellido), ',', TRIM(opb.opb_nombre), ' ', TRIM(opb.opb_snombre))
											AS gan_razsoc, -- armo el nombre, esto lo debería hacer la grabación de la orden de pago
					TRIM(opb.opb_domi_calle) AS gan_domi_calle,
					CASE 
						WHEN TRIM(COALESCE(opb.opb_domi_nro, '0')) = '0' 	
							THEN ''
							ELSE TRIM(opb.opb_domi_nro)
					END AS gan_domi_nro, 
					CASE 
						WHEN TRIM(COALESCE(opb.opb_domi_piso, '0')) = '0' 	
							THEN ''
							ELSE TRIM(opb.opb_domi_piso)
					END AS gan_domi_piso,
					CASE 
						WHEN TRIM(COALESCE(opb.opb_domi_dpto, '0')) = '0' 	
							THEN ''
							ELSE TRIM(opb.opb_domi_dpto)
					END AS gan_domi_dpto,
					opb.tbl_localidades_id_c AS tbl_localidades_id_c,
					opb.tbl_provincias_id_c AS tbl_provincias_id_c,
					opb.opb_extranjero AS gan_extranjero,
					opb.opb_provincia_extranjera AS gan_provincia_extranjera,
					opb.opb_localidad_extranjera AS gan_localidad_extranjera,
					opb.opb_cpos_extranjero AS gan_cpos_extranjero,
					opb.tbl_paises_id_c AS tbl_paises_id_c
						FROM pre_premios pp
							JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pp.id AND COALESCE(opp.pre_premios_cuotas_id_c, '') = '' AND opp.deleted = 0
							JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'B' AND opb.deleted = 0
							JOIN pre_tipo_comprobante tc ON tc.id = opp.pre_tipo_comprobante_id_c AND tc.deleted = 0
						WHERE pp.id = par_Premios_ID_C
						 AND opb.opb_cuit IS NOT NULL -- Agrego RL: si el cuit queda en null QUE SALTEEEE
					;
		END IF;
					
		-- Si es brinco sueldo
		IF v_idJuego = var_brinco AND v_pre_tipopremio = var_brincosueldo THEN
		    -- ajusto la fecha y el beneficiario!
			SET @etapa = 8;
			INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
			UPDATE pre_premios_cuotas pc
					JOIN pre_premios pp ON pp.`id` = pc.`pre_premios_id_c`
					JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pp.id AND COALESCE(opp.pre_premios_cuotas_id_c, '') = '' AND opp.deleted = 0
					JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'B' AND opb.deleted = 0
				SET 	pc.prc_tipo             = opb.opb_tipo, -- tipo de persona (fisica / juridica)
					pc.prc_cuit             = opb.`opb_cuit`, -- cuit
					pc.prc_nombre           = CONCAT(TRIM(opb.`opb_apellido`), ', ', TRIM(opb.`opb_nombre`), ' ', TRIM(opb.`opb_snombre`)),
					pc.prc_doc_tipo         = COALESCE(opb.`opb_tdoc`, ''), 
					pc.prc_doc_nro          = COALESCE(opb.`opb_doc_nro`,0),
					pc.prc_domi_calle       = COALESCE(opb.`opb_domi_calle`, ''), 
					pc.prc_domi_nro         = COALESCE(opb.`opb_domi_nro`, ''),
					pc.prc_domi_piso        = COALESCE(opb.`opb_domi_piso`, ''),
					pc.prc_domi_dpto        = COALESCE(opb.`opb_domi_dpto`, ''),
					pc.prc_domi             = COALESCE(opb.`opb_domi`, ''), 
					pc.prc_telefono_carac   = COALESCE(opb.`opb_telefono_carac`, 0), 
					pc.prc_telefono_numero  = COALESCE(opb.`opb_telefono_numero`, 0), 
					pc.prc_email            = COALESCE(opb.`opb_email`, ''), 
					pc.tbl_localidades_id_c = COALESCE(opb.`tbl_localidades_id_c`, ''),
					pc.tbl_provincias_id_c  = COALESCE(opb.`tbl_provincias_id_c`, ''),
					pc.tbl_paises_id_c      = COALESCE(opb.`tbl_paises_id_c`, ''),
					pc.`prc_estado_pago`    = 'P'
				WHERE pc.`pre_premios_id_c` = par_Premios_ID_C
				 AND opb.opb_cuit IS NOT NULL -- Agrego RL: si el cuit queda en null QUE SALTEEEE
				;
		    
		END IF;
*/			
		-- elimino las relaciones BENEFICIARIOS - OP (la OP tiene el mismo id que el premio!)
		SET @etapa = 9;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		DELETE r FROM `pre_orden_pago_beneficiarios_pre_orden_pago_c` r
			WHERE r.`pre_orden_pago_beneficiarios_pre_orden_pagopre_orden_pago_ida` = par_Premios_ID_C;
		
		-- creo la relacion Beneficiario
		SET @etapa = 10;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		INSERT INTO `pre_orden_pago_beneficiarios_pre_orden_pago_c` (`id`, `date_modified`, `deleted`,
				`pre_orden_pago_beneficiarios_pre_orden_pagopre_orden_pago_ida`, `pre_orden_79d1ciarios_idb`)
			SELECT opb.id, NOW(), 0, opb.`pre_orden_pago_id_c`, opb.id
				FROM `pre_orden_pago_beneficiarios` opb 
				WHERE opb.`pre_orden_pago_id_c` = par_Premios_ID_C;
				
		
		SET @etapa = 11;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		COMMIT;
			
		-- ****************************************** FINAL DE TRANSACCION *****************************************
		/*
		-- Invoco Calculo de Intereses
		SET @etapa = 11;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa);
		CALL PREMIOS_Calculo_Interes(par_Premios_ID_C);
		*/
		
		SET @etapa = 12;
		INSERT INTO kk (nombre) SELECT CONCAT(NOW(), ' - confirma premios-id: ', par_Premios_ID_C, ' - ETAPA ', @etapa, ' - Id Mensaje:', v_id_mensaje);
		-- Si no ocurrió un error
		IF v_id_mensaje = 0 THEN
			SET hecho = TRUE, v_id_mensaje = 1, v_mensaje = 'Confirmación datos del premio satisfactoria.';
		END IF;
	END;
	-- Retorno ok
	
	IF @echo = 'on' THEN
		SELECT COALESCE(v_tbl_Provincias_ID_C, '') 	AS provincia, 
			COALESCE(v_idJuego,0) 			AS juego, 
			COALESCE(v_nroSorteo, 0) 		AS sorteo, 
			COALESCE(v_pre_secuencia, 0) 		AS secuencia, 
			hecho 					AS ejecOk, 
			v_id_mensaje 				AS idMsg, 
			v_mensaje 				AS msg, 
			COALESCE(v_codigo_autorizacion, '') 	AS codigoAutorizacion;
	END IF;
	
END$$

DELIMITER ;