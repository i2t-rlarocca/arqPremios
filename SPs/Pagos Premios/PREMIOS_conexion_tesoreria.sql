DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_conexion_tesoreria`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_conexion_tesoreria`(
		  IN opp_id CHAR(36), 
		  OUT RCode INT, 
		  OUT RTxt LONGTEXT,
		  OUT RId VARCHAR (36),
		  OUT RSQLErrNo INT,
		  OUT RSQLErrtxt VARCHAR (500)  
)
    COMMENT 'Vuelca el premio a tesoreria'
thisSP:BEGIN
DECLARE ID_Proveedor 		VARCHAR(36);
DECLARE ID_Liquidacion 		VARCHAR(36);
DECLARE ID_OrdenPago 		VARCHAR(36);
DECLARE ID_ModeloImpuestoG 	VARCHAR(36);
DECLARE ID_ImpuestoG 		VARCHAR(36);
DECLARE ID_ReferenciaContableG 	VARCHAR(36);
DECLARE ID_ModeloImpuestoCU 	VARCHAR(36);
DECLARE ID_ImpuestoCU 		VARCHAR(36);
DECLARE ID_ReferenciaContableCU VARCHAR(36);
DECLARE AlicuotaG 		DECIMAL(18,6);
DECLARE AlicuotaCU 		DECIMAL(18,6);
DECLARE TG01_categoriareferente_id_c CHAR(36);
DECLARE par_idUsuario		VARCHAR(36); -- ID_Usuario es lo mismo.
DECLARE cuitBOOL 		VARCHAR(36);
DECLARE mi_valor 		INT (10);
DECLARE cuitIN 			VARCHAR(36);
DECLARE tipoReferenteIN 	VARCHAR(10);
	
DECLARE	idProveedor 		CHAR(36);
DECLARE prov_codigo 		INT(10);
DECLARE p_name 			VARCHAR(150);
DECLARE p_nombre_fantasia_c 	VARCHAR(255);
DECLARE p_tipo_doc 		CHAR(36);
DECLARE p_nro_doc		INT(11);
DECLARE p_fac_calle		VARCHAR(150);
DECLARE p_fac_ciudad		VARCHAR(100);
DECLARE p_fac_prov		VARCHAR(100);
DECLARE p_fac_cp 		VARCHAR(20);
DECLARE p_fac_pais		VARCHAR(255);
DECLARE p_env_calle 		VARCHAR(150);
DECLARE p_env_ciudad		VARCHAR(100);
DECLARE p_env_prov 		VARCHAR(100);
DECLARE p_env_cp 		VARCHAR(20);
DECLARE p_env_pais		VARCHAR(255);
DECLARE p_tel_1			VARCHAR(100);
DECLARE p_tel_2			VARCHAR(100);
DECLARE p_tel_3			VARCHAR(100);
DECLARE p_email			VARCHAR(255);
DECLARE p_obs 			TEXT;
DECLARE p_categoria 		CHAR(36);
DECLARE p_zona			CHAR(36);
DECLARE p_vendedor		CHAR(36);
DECLARE p_cobrador		CHAR(36);
DECLARE p_lim_cred		DECIMAL(18,2);
DECLARE p_lista_precio		CHAR(36);
DECLARE p_cond_comercializacion	CHAR(36);
DECLARE p_partida_pres_default	CHAR(36);
DECLARE p_ref_contable_default 	CHAR(36);
DECLARE p_situacion_iva		CHAR(36);
DECLARE p_cuit_c		VARCHAR(13);
DECLARE p_cai			VARCHAR(255);
DECLARE p_fecha_vto_cai 	VARCHAR(255);
DECLARE p_cuit_exterior 	VARCHAR(255);
DECLARE p_id_impositivo		VARCHAR(255);
DECLARE p_rel_estado		VARCHAR(100);
DECLARE p_rel_categoria_bloqueo	CHAR(36);
DECLARE p_rel_tipo_comprobante	CHAR(36);
DECLARE p_id_localidad_facturacion	CHAR(36);
DECLARE p_id_localidad_envio	CHAR(36);
DECLARE p_id_acreedor           CHAR(36) DEFAULT 0; 
DECLARE p_estado_c              CHAR(36);
DECLARE p_categoria_referente   CHAR(36);
DECLARE cuitProveedor 		VARCHAR(13);
DECLARE tipoRef 		VARCHAR(100);
DECLARE idProv 			CHAR(36);
DECLARE cod_c 			INT(10);
DECLARE idLiquidacion           CHAR(36);
-- cursor de minutas
DECLARE minutas_contables CURSOR FOR
	SELECT 	mc.referenciacontable AS referenciacontable, 
		mc.cuentainterna AS cuentainterna, 
		mc.debehaber AS debehaber, 
		mc.concepto AS concepto, 
		mc.idminuta AS idminuta
	FROM pre_minuta_contable mc
		INNER JOIN pre_orden_pago opp
		INNER JOIN pre_premios b ON b.id = opp.pre_premios_id_c
		INNER JOIN sor_pgmsorteo c ON c.id = b.sor_pgmsorteo_id_c
	WHERE mc.sor_producto_id_c = c.sor_producto_id_c -- '4' -- es el producto de la tabla de premios asociada a la orden de pago
	AND mc.deleted = 0
	AND mc.pre_canal_de_pago_id_c = 'T'
	AND opp.id = opp_id -- de la orden de pago
	ORDER BY mc.idminuta, mc.debehaber, mc.referenciacontable;
	
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
	SET RSQLErrNo = @errno;
	SET RSQLErrtxt = CONCAT("PREMIOS_conexion_tesoreria ", @msg);
		
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - FIN CON ERROR - etapa ', @etapa, ' - Orden de Pago:', opp_id, 
			' - code:', @code, ' - errno:', @errno, ' - msg:', @msg));
END;
	
SET RCode = 1;
SET RTxt = 'OK';
SET RId = 0;
SET RSQLErrNo = 0;
SET RSQLErrtxt = "OK";
-- insert del comprobante orden de pago - ok
-- insert del comprobante retencion - ok
-- insert relacion de orden de pago y retencion - ok
-- insert minuta contable en la orden de pago (solo canal de pago tesoreria) - ok
-- insert de los datos de la retencion de ganancias - ok
-- carga parametros
SELECT 	par_ID_ModeloImpuestoG,	
	par_ID_ImpuestoG, 
	par_ID_ReferenciaContableG, 
	par_ID_ModeloImpuestoCU, 
	par_ID_ImpuestoCU, 
	par_ID_ReferenciaContableCU,
	par_alicuotaG,
	par_alicuotaG, 
	par_tg01_categoriareferente_id
INTO 	ID_ModeloImpuestoG,	
	ID_ImpuestoG, 
	ID_ReferenciaContableG, 
	ID_ModeloImpuestoCU, 
	ID_ImpuestoCU, 
	ID_ReferenciaContableCU,
	AlicuotaG,
	AlicuotaCU,
	TG01_categoriareferente_id_c
FROM pre_parametros;
		
SET @etapa = 0, @cuit = NULL, par_idUsuario = '1', @idOrdenPago = '', @idcomprobantepremio = '' ; -- ADEN - 2025-05-07 - se agrega inicializacion de las variables @idOrdenPago y @idcomprobantepremio para condicionar el UPDATE
SET ID_OrdenPago = opp_id;
	
INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - 1 - OP - ', @etapa, ' - Orden de Pago:', opp_id));
	
/* rutina para informapremiosAS.php
SELECT DISTINCT(p.id) AS idPrm, op.name, op.opp_fecha_pago FROM pre_premios p
INNER JOIN pre_orden_pago op ON op.pre_premios_id_c = p.id AND op.deleted = 0
WHERE p.pre_estregistrobenef = 'C' AND COALESCE(op.estado_envio_as400, 'P') = 'P' 
AND op.opp_fecha_comprobante > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) 
AND (op.pre_canal_de_pago_id_c = 'T' OR 
((COALESCE(op.opp_ret_ley20630, 0) 
+ COALESCE(op.opp_ret_ley23351, 0) 
+ COALESCE(op.opp_ret_ley11265, 0) 
+ COALESCE(op.opp_ret_otros, 0)) <> 0))
AND op.opp_fecha_pago IS NOT NULL
ORDER BY op.name;
*/
	
SELECT 	opb.opb_cuit, opb.opb_apellido, opb.opb_apemat, opb.opb_nombre, opb.opb_snombre, 
	opb.opb_tdoc, opb.opb_doc_nro, opb.opb_domi, COALESCE(opb.opb_email, '') AS email, 
	opp.name AS nliq, 
        -- Agrega AE - 2023-09-29 para que fije fecha de pago cuando es tesoreria si no viene!
	IF(opp.pre_canal_de_pago_id_c = 'T', COALESCE(opp.opp_fecha_pago, CURDATE()), opp.opp_fecha_pago), 
	opp.opp_fecha_comprobante, 
	opp.opp_impneto, 
	opp.opp_impbruto, 
	opp_impimponible AS imponible, 
	COALESCE(opp.opp_ret_ley20630, 0) + COALESCE(opp.opp_ret_ley23351, 0) AS ret_gan, 
	opp.opp_ret_ley11265 AS ret_cudaio, 
	opp.opp_ret_otros,
	opp.opp_estado_registracion, opp.estado_envio_as400, 
	opp.opp_fpago, -- agregado ADEN - 2024-11-07
	COALESCE(u.id, '1'), COALESCE(u.user_name, 'admin'), pre_canal_de_pago_id_c,
	c.id AS idSorteo, -- agregado
	c.nrosorteo, c.sor_producto_id_c,
	-- datos de localidad, provincia, pais del beneficiario.
	opb.tbl_localidades_id_c,
	loc.name AS localidad,
	loc.loc_cpos AS codigopostal,
	opb.tbl_provincias_id_c,
	pro.name AS provincia,
	opb.tbl_nacionalidades_id_c,
	nac.name AS pais,
	b.pre_pagaagencia AS pre_pagaagencia,
	d.id_juego, 
	d.modo_acred_premios   -- 2024-12-30 - ADEN - para verificar si hay que generar un debito en la cta cte si paga_agencia = 'S' y pagó TESORERÍA
INTO 	@cuit, @apellido, @apemat, @nombre, @snombre, 
	@tdoc, @ndoc, @domi, @email,
	@nliq, 
	@fpago, -- fecha de comprobante
	@fcomp,     
	@impneto,
	@impbruto, 
	@imponible, 
	@ret_gan, 
	@ret_cudaio, 
	@ret_otros, 
	@estreg, @estenv,
	@forma_pago_solicitada, -- agregado ADEN - 2024-11-07
	@uid, @user_name, @canal_pago, 
	@idSorteo, -- agregado
	@nrosorteo, @id_producto,
	@id_localidad,
	@nombreLocalidad,
	@locCodigoPostal,
	@id_provincia,
	@nombreProvincia,
	@id_pais,
	@nombrePais,
	@pagaagencia,
	@id_juego, 
	@modo_acred_premios  -- 2024-12-30 - ADEN - para verificar si hay que generar un debito en la cta cte si paga_agencia = 'S' y pagó TESORERÍA
FROM pre_orden_pago opp
INNER JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = opp.id AND opb.opb_tipobeneficiario = 'B'
INNER JOIN pre_premios b ON b.id = opp.pre_premios_id_c		
INNER JOIN sor_pgmsorteo c ON c.id = b.`sor_pgmsorteo_id_c`
INNER JOIN sor_producto d ON d.id = c.sor_producto_id_c
LEFT JOIN users u ON u.id = opp.assigned_user_id
LEFT JOIN tbl_localidades loc ON loc.id = opb.tbl_localidades_id_c
LEFT JOIN tbl_provincias pro ON pro.id = opb.tbl_provincias_id_c
LEFT JOIN tbl_nacionalidades nac ON nac.id = opb.tbl_nacionalidades_id_c
WHERE opp.id = opp_id;
	
SET @etapa = @etapa + 1;
INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - 2 - valido - ', @etapa, ' - Orden de Pago:', opp_id));
INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - 2 - fpago - ', @fpago,' estreg ', @estreg, ' estenv ', @estenv));
INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - 2 - retencion - ', @ret_gan+@ret_cudaio+@ret_otros,' canal pago ', @canal_pago));
IF (@cuit IS NULL) THEN  
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion - Falta el dato del beneficiario - ', @etapa, ' - Orden de Pago:', opp_id));
	
	SET 	RCode      = -10,
		RTxt       = 'Falta el dato del beneficiario';
	
	LEAVE thisSP;
END IF;
	
IF (@fpago IS NULL) THEN 
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion - Fecha de pago no informada - ', @etapa, ' - Orden de Pago:', opp_id));
	
	SET 	RCode      = -11,
		RTxt       = 'Fecha de pago no informada';
		
	LEAVE thisSP;
END IF;
IF (@estreg != 'D') THEN
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion - Estado de registración:  La liquidación no esta confirmada - ', @etapa, ' - Orden de Pago:', opp_id));
	
	SET 	RCode      = -12,
		RTxt       = 'Estado de registración:  La liquidación no esta confirmada';
		
	LEAVE thisSP;
END IF;
	
IF (@estenv != 'P') THEN
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion - Estado de envío: La liquidación ya fue pagada - ', @etapa, ' - Orden de Pago:', opp_id));
	
	SET 	RCode      = -13,
		RTxt       = 'Estado de envío: La liquidación ya fue pagada';
		
	LEAVE thisSP;
END IF;
/*
IF ((@ret_gan+@ret_cudaio+@ret_otros) = 0 OR @canal_pago <> 'T')  THEN
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion - ', @etapa, ' - Orden de Pago:', opp_id));
	SET 	RCode      = -5,
		RTxt       = 'El premio no tiene retenciones o no es canal Tesoreria';
	
	LEAVE thisSP;
END IF;
*/
-- si el canal es tesoreria, tienen que estar registrados y aprobados los documentos
IF (@canal_pago = 'T' 
-- ADEN - 2024-11-07 - SALVO EL PREMIO ESTIMULO DEL TELEKINO PASADO POR CTACTE
	AND NOT (@id_juego = 73 AND @forma_pago_solicitada = 4)) THEN
		SET @estado_doc = 'B';
		SELECT COALESCE(opd_estado, '') INTO @estado_doc FROM pre_orden_pago a
		INNER JOIN pre_orden_pago_documentos b ON  a.id = b.pre_orden_pago_id_c AND b.deleted = 0
		WHERE a.id = opp_id;
		
		IF @estado_doc <> 'A' THEN
			INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion - Estado de envío: La liquidación sin documentos aprobados - ', @etapa, ' - Orden de Pago:', opp_id));
			
			SET 	RCode      = -20,
				RTxt       = 'Estado de envío: La liquidación fue confirmada. Ingrese y apruebe los documentos digitales para efectivizar el pago';
				
			LEAVE thisSP;
		END IF;
END IF;
-- 01-Inicio. Referente y liquidacion si canal tesoreria, otras provincias y agencia cuando para agencia
--            el caso donde no genera liquidacion es doble chance porque acredita en cuenta corriente
-- ADEN - 2024-11-07 - hay que usar FORMA DE PAGO, 2=transferencia o 4=cuenta corriente y no el engendro del CODIGO DE JUEGO!!!
-- IF (@canal_pago = 'T' AND @id_juego <> 41) 
IF ((@canal_pago = 'T' AND @forma_pago_solicitada <> 4) 
	OR (@canal_pago = 'O') 
	OR (@pagaagencia = 'S' AND @canal_pago = 'A')) THEN
	
	-- recupera la caja de santa fe
	SET @caja_santafe = '';
	SELECT a.caja_santafe 
	INTO @caja_santafe 
	FROM cas02_cc_parametros a
	INNER JOIN `suitecrm_administracion_2019`.tg01_cajas b ON b.id = a.caja_santafe
	LIMIT 1;
	IF (@caja_santafe = '')  THEN
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' Caja Santa Fe no existe', @etapa, ' - Orden de Pago:', opp_id));
		
		SET 	RCode      = -1,
			RTxt       = 'La caja Santa Fe no existe';
			
		LEAVE thisSP;
	END IF;
	-- PP-00001
	-- incorpora al beneficiario del premio, como sujeto, del tipo proveedores eventuales
	SET @etapa = @etapa + 1;
	INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - 3 - cuit - ', @cuit));
	-- CALL `suitecrm_administracion_2019`.`SP_ET_referente_INS-UPD`(@cuit, 'OP', @RCode, @RTxt, @idReferente, @RSQLErrNo, @RSQLErrtxt);
	SET cuitIN = @cuit, tipoReferenteIN = 'OP';
	-- cuitBOOL `suitecrm_administracion_2019`
	SELECT IF(COUNT(cuit_c)=0,NULL,cuit_c) 
	INTO cuitBOOL   -- ajuste para null
	FROM `suitecrm_administracion_2019`.accounts_cstm 
	WHERE cuit_c = cuitIN AND tiporeferente_c = 'OP';
	SELECT MAX(codigo_c)+1 
	INTO mi_valor
	FROM `suitecrm_administracion_2019`.accounts_cstm;
	-- cant 0 insert en ambas tablas
	-- cant 1 insert en alguna de las tablas, el campo bd donde si existe.
	-- cant 2 update
	-- cuitBOOL - NO es null, es porque está en `suitecrm_administracion_2019`
	IF cuitBOOL IS NOT NULL THEN
		-- if ((@cant = 1 && @basededatos = 'etangram') || (@basededatos = 'etangram' && @cant = 2)) then 
		SELECT 	sc.cuit_c, 				sc.tiporeferente_c,	sc.codigo_c, 			sc.nombre_fantasia_c, 
			sc.tg01_tipodocumento_id_c,
			sc.documento_c,		 		sc.email_c, 		sc.tg01_zonas_id_c,		sc.tg01_vendedores_id_c,
			sc.tg01_vendedores_id_c,		sc.limitecredito_c,	tglp_tg_listasprecios_id_c,	sc.tg01_condicioncomercial_id_c,
			sc.tg05_partidas_presupuestaria_id_c,	sc.tg01_referenciascontables_id_c,
			sc.tg01_categoriasiva_id_c,		sc.tg01_categoriasiva_id_c,		
			sc.cai_c,				fvtocai_c,		cuitexterior_c,			idimpositivo_c,	
			tg01_categoriabloqueo_id_c, 		tg01_tipocomprobante_id_c,				tg01_acreedor_id_c,
			sc.estado_c, sc.tg01_categoriareferente_id_c, id_c
		
		INTO 	cuitProveedor, 				tipoRef, 		cod_c, 				p_nombre_fantasia_c, 		
			p_tipo_doc,
			p_nro_doc, 				p_email, 		p_zona, 			p_vendedor,
			p_cobrador, 				p_lim_cred, 		p_lista_precio,			p_cond_comercializacion,
			p_partida_pres_default,			p_ref_contable_default,
			p_situacion_iva,			p_situacion_iva,			
			p_cai,					p_fecha_vto_cai,	p_cuit_exterior,		p_id_impositivo,
			p_rel_categoria_bloqueo,		p_rel_tipo_comprobante,					p_id_acreedor,
			p_estado_c,
			p_categoria, idProveedor
		
		FROM `suitecrm_administracion_2019`.accounts_cstm sc 
		JOIN `suitecrm_administracion_2019`.accounts a ON sc.id_c = a.id
		WHERE sc.cuit_c = cuitIN AND sc.tiporeferente_c = tipoReferenteIN 
		LIMIT 1;		
	END IF;
	-- ALTA
	IF cuitBOOL IS NULL THEN
		
		SET p_cond_comercializacion = '';
		SET p_lista_precio = '';
		
		SELECT tg01_condicioncomercial_id1_c, tglp_tg_listasprecios_id_c 
		INTO  p_cond_comercializacion, p_lista_precio
		FROM `suitecrm_administracion_2019`.tg01_parametros
		WHERE NAME = 'etangram';
		
		CALL `suitecrm_administracion_2019`.SP_ET_proveedoresINS(
			tipoReferenteIN, 	mi_valor, 		
			CONCAT(@apellido, ', ',@nombre,' ', @snombre), -- p_name,
			CONCAT(@apellido, ', ',@nombre,' ', @snombre),  -- p_nombre_fantasia_c, 
			@tdoc, -- p_tipo_doc, 		
			@ndoc, -- p_nro_doc,		
			@domi, -- p_fac_calle,
			@nombreLocalidad, -- p_fac_ciudad, 
			@nombreProvincia, -- p_fac_prov,
			@locCodigoPostal, 		
			@nombrePais, -- p_fac_pais, 		
			@domi, 
			@nombreLocalidad, @nombreProvincia, @locCodigoPostal, 		
			@nombrePais, '', '','', @email, '', '99',  '', 	'', '', 0, p_lista_precio, p_cond_comercializacion, 
			'', '',	'0', 	@cuit,	'','', 	'','',	'0', '', '','',	'',   '0', 
			0,		
			@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
				
		IF (@RCode <> 1) THEN
			SET 	RCode      = 1,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;
				
			LEAVE thisSP;
		END IF;
		
		SET RId = @RId;
	END IF;
	IF cuitBOOL IS NOT NULL THEN	
		CALL `suitecrm_administracion_2019`.SP_ET_proveedoresUDP(
			idProveedor,	 		tipoReferenteIN, 	cod_c, 		
			CONCAT(@apellido, ', ',@nombre,' ', @snombre),
			CONCAT(@apellido, ', ',@nombre,' ', @snombre), 
			p_tipo_doc, 		p_nro_doc,	
			@domi, 
			@nombreLocalidad, 	@nombreProvincia, 	@locCodigoPostal,  	@nombrePais, 
			@domi, 			@nombreLocalidad, 	@nombreProvincia, 	@locCodigoPostal, 
			@nombrePais, 			p_tel_1, 		p_tel_2,	p_tel_3,
			@email, 			p_obs, 			p_categoria, 	p_zona, 
			p_vendedor, 			p_cobrador, 		p_lim_cred, 	p_lista_precio, 
			p_cond_comercializacion,	p_partida_pres_default, p_ref_contable_default,
			p_situacion_iva,		cuitProveedor, 		p_cai, 		p_fecha_vto_cai, 
			p_cuit_exterior, 		p_id_impositivo,	p_rel_estado,	p_rel_categoria_bloqueo, 
			p_rel_tipo_comprobante,		p_id_localidad_facturacion,		p_id_localidad_envio, 
			p_id_acreedor, NULL,
			@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt);
		
		IF (@RCode <> 1) THEN	
			INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla validacion referentes (SP_ET_referente_INS-UPD) - ', @etapa, ' - Orden de Pago:', opp_id, ' - Cuit:', @cuit ));
		
			SET 	RCode      = 2,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;
				
			LEAVE thisSP;
		END IF;
		
		SET RId = @RId;		
	END IF;
		
	SET @idReferente = RId;
			
	START TRANSACTION;
	
	-- toma la caja por defecto de la organizacion
	SET p_cond_comercializacion = '';
	
	SELECT tg01_condicioncomercial_id1_c 
	INTO p_cond_comercializacion  
	FROM `suitecrm_administracion_2019`.tg01_parametros p
	WHERE NAME = 'etangram';
	
	-- seteo SP_ET_LiquidacionINS -- tipo -> 'PR' premios para tesoreria, sino R retencion
	IF @canal_pago = 'T' THEN 
		SET 	@Tipo = 'PR';
	ELSE
		SET 	@Tipo = 'R';
	END IF;
	
	SET 	@ID_Depositos = '', @ID_Areas = '';
		
	-- busca la caja de retenciones para lo que no es tesoreria
	SET @idCaja_ret = '';
	SELECT COALESCE(tg01_cajas_id_c,'')  
	INTO @idCaja_ret 
	FROM `suitecrm_administracion_2019`.tg01_parametros
	WHERE NAME = 'retenciones';
	
	-- seteo estado -> dependiendo si:
	-- si canal de pago es tesorería -> provisorio ; sino definitivo
	IF (@canal_pago = 'T') THEN 
		SET @estado = 'Provisorio';	
		SET @estado_comp = 'B'; -- borrador
		SET @caja_std = @caja_santafe;
	ELSE 
		SET @estado = 'Definitivo';
		SET @estado_comp = 'D'; -- definitivo
		SET @caja_std = @idCaja_ret;
	END IF;
	
	-- PP-00002
	-- genera liquidacion
	INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio SP_ET_LiquidacionINS - nliq: ',@nliq, ' - estado: ', @estado, ' - idCaja: ', @caja_std, ' - ID_Proveedor: ', @idReferente, ' - par_idUsuario', par_idUsuario, ' - tipo:', @Tipo, 
							' - ID_Depositos', @ID_Depositos, ' - ID_Areas' ,@ID_Areas, ' - impbruto', @impbruto); 
							
	CALL `suitecrm_administracion_2019`.SP_ET_LiquidacionINS(	@nliq, @fpago, @estado, @caja_std, @idReferente, par_idUsuario, @fpago, @fpago, @Tipo, 
							@ID_Depositos, @ID_Areas, @impbruto, '', '',
							'','', -- caja origen, caja destino
							@RCode, @RTxt, @RId_Liquidacion, @RSQLErrNo, @RSQLErrtxt);
	
	-- Errores
	IF (@RCode <> 1) THEN 
		ROLLBACK;
		
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' PREMIOS_conexion_tesoreria - falla insercion de liquidacion - ', @etapa, 
									' - Orden de Pago:', opp_id, ' - Liquidacion:', @nliq, ' - Cuit:', @cuit ));
		SET 	RCode      = 3,
			RTxt       = @RTxt,
			RId        = '',
			RSQLErrNo  = @RSQLErrNo,
			RSQLErrtxt = @RSQLErrtxt;
			
		LEAVE thisSP;	
	END IF;
	
	-- seteo liquidacion 
	SET ID_Liquidacion = @RId_Liquidacion;
		
	-- actualiza el numero operacion, para luego hacer el delete
	UPDATE `suitecrm_administracion_2019`.tg12_liquidacion 
	SET nro_liquidacion = @nliq
	WHERE id = @RId_Liquidacion;
	
	/*  creacion de orden de pago*/
		
	-- SP_ET_CabeceraINS - ORDEN DE PAGO - PTipocbte
	SET @idTipoComp = '';
	SET @tipooperacion_caja  = '';
	
	SELECT id, tg01_tipooperacion_id_c 
	INTO @idTipoComp,  @tipooperacion_caja 
	FROM `suitecrm_administracion_2019`.tg01_tipocomprobante
	WHERE idtipocomp = 'OP'
	AND deleted = 0;
	-- SP_ET_CabeceraINS - ORDEN DE PAGO - id_talonario
	SET @idTalonario = '';
	-- Set @fechaCpte = CURDATE(), @FechaBase = curdate(), @FechaContable = curdate();
	SET @fechaCpte = DATE_FORMAT(@fpago, '%Y-%m-%d'); 
	SET @FechaBase = DATE_FORMAT(@fpago, '%Y-%m-%d'); 
	SET @FechaContable = DATE_FORMAT(@fpago, '%Y-%m-%d');
	
	INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio SP_ET_CabeceraINS'); 
	
	-- SP_ET_CabeceraINS - ORDEN DE PAGO
	CALL `suitecrm_administracion_2019`.SP_ET_CabeceraINS (
		  @idTipoComp     	-- tipo OP         
		, NULL			-- PModelocbte             
		, @nliq			-- PNroCbte                
		, @impbruto		-- PImporteTotalcbte es el parametro de entrada?     
		, @fechaCpte		-- PFechacbte             
		, @idReferente           -- PidProveedor        
		, NULL 			-- PCAE                    
		, NULL			-- PFormaPago              
		, NULL 			-- PidcondComercializacion 
		, '' 					-- PIdSucursal              -- si no seenvia toda de parametros
		, ''				 	-- PIdDeposito              -- si no seenvia toda de parametros
		, '' 					-- PIdLista                 -- si no seenvia toda de parametros
		, '' 					-- PIdMoneda                -- si no seenvia toda de parametros
		, NULL 					-- PCotizacion             
		, @FechaBase 				-- PFechaBase             
		, @FechaContable 			-- PFechaContable          
		, @caja_std				-- Pidcaja                  -- si no seenvia toda de parametros
		, par_idUsuario		   		-- PidUsuario es el parametro de entrada?              
		, CONCAT('Pago Premios: ',@nliq)   	-- PObservaciones @PObservaciones         
		, @estado_comp				-- EstadoComprobante       
		, NULL					-- ID_Expediente           
		, @idTalonario 				-- id_talonario            
		, NULL 					-- id_remesa               
		, ID_Liquidacion			-- id_liquidacion    
		, 0                                  	-- PDA
		, @RCode
		, @RTxt
		, @RId
		, @RSQLErrNo
		, @RSQLErrtxt
	);
	
	-- Errores
	IF (@RCode <> 1) THEN
		ROLLBACK;
		
		SET 	RCode      = @RCode,
			RTxt       = @RTxt,
			RId        = '',
			RSQLErrNo  = @RSQLErrNo,
			RSQLErrtxt = @RSQLErrtxt;
			
		LEAVE thisSP ;
	END IF;
	SET @idOrdenPago = @RId;
	
	INSERT INTO kk_auditoria (nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin CALL SP_ET_CabeceraINS - ORDEN DE PAGO - idOrdenPago :',@idOrdenPago);	
	
	/* fin creacion orden de pago*/
	/* minuta contable si es TESORERIA */
	
	IF (@canal_pago = 'T') THEN 
		-- genera el comprobante de premios para asociarlo a la orden de pago (similar al circuito de pago a proveedores)
		SET @idTipoComp = '';
		SET @tipooperacion  = '';
		
		SELECT id, tg01_tipooperacion_id_c 
		INTO @idTipoComp,  @tipooperacion 
		FROM `suitecrm_administracion_2019`.tg01_tipocomprobante
		WHERE idtipocomp = 'PRT'
		AND deleted = 0;
		SET @idTalonario = '';
		-- SET @fechaCpte = CURDATE(), @FechaBase = CURDATE(), @FechaContable = CURDATE();
		SET @fechaCpte = DATE_FORMAT(@fpago, '%Y-%m-%d'); 
		SET @FechaBase = DATE_FORMAT(@fpago, '%Y-%m-%d'); 
		SET @FechaContable = DATE_FORMAT(@fpago, '%Y-%m-%d');
		
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio SP_ET_CabeceraINS - NDI'); 
		
		-- SP_ET_CabeceraINS - NDI
		CALL `suitecrm_administracion_2019`.SP_ET_CabeceraINS (
			  @idTipoComp     	-- tipo OP         
			, NULL			-- PModelocbte             
			, @nliq			-- PNroCbte                
			, @impbruto		-- PImporteTotalcbte es el parametro de entrada?     
			, @fechaCpte		-- PFechacbte             
			, @idReferente           -- PidProveedor        
			, NULL 			-- PCAE                    
			, NULL			-- PFormaPago              
			, p_cond_comercializacion -- PidcondComercializacion 
			, '' 					-- PIdSucursal              -- si no seenvia toda de parametros
			, ''				 	-- PIdDeposito              -- si no seenvia toda de parametros
			, '' 					-- PIdLista                 -- si no seenvia toda de parametros
			, '' 					-- PIdMoneda                -- si no seenvia toda de parametros
			, NULL 					-- PCotizacion             
			, @FechaBase 				-- PFechaBase             
			, @FechaContable 			-- PFechaContable          
			, '' 					-- Pidcaja                  -- si no seenvia toda de parametros
			, par_idUsuario		   		-- PidUsuario es el parametro de entrada?              
			, CONCAT('Pago Premios: ',@nliq)   	-- PObservaciones @PObservaciones         
			, 'B'					-- EstadoComprobante       
			, NULL					-- ID_Expediente           
			, @idTalonario 				-- id_talonario            
			, NULL 					-- id_remesa               
			, ID_Liquidacion			-- id_liquidacion    
			, 0                                  	-- PDA
			, @RCode
			, @RTxt
			, @RId
			, @RSQLErrNo
			, @RSQLErrtxt);
		
		-- Errores
		IF (@RCode <> 1) THEN
			ROLLBACK;
					
			SET 	RCode      = @RCode,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;
				
			LEAVE thisSP ;
		END IF;
		SET @idcomprobantepremio = @RId;
	
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin CALL SP_ET_CabeceraINS - NDI - idcomprobantepremio :',@idcomprobantepremio);	
		-- inserta la relacion
		INSERT INTO `suitecrm_administracion_2019`.tg03_comprobantes_tg03_comprobantes_c (
				    id, date_modified, deleted
				    , `tg03_comprobantes_tg03_comprobantestg03_comprobantes_ida` 
				    , `tg03_comprobantes_tg03_comprobantestg03_comprobantes_idb`)
		VALUES (UUID(), NOW(), 0, @idcomprobantepremio, @idOrdenPago);
		
		INSERT INTO `suitecrm_administracion_2019`.tg03_comprobantes_tg03_comprobantes_c (
				    id, date_modified, deleted
				    , `tg03_comprobantes_tg03_comprobantestg03_comprobantes_ida` 
				    , `tg03_comprobantes_tg03_comprobantestg03_comprobantes_idb`)
		VALUES (UUID(), NOW(), 0,  @idOrdenPago, @idcomprobantepremio);																
	
		-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
		-- recupera subdiario para la minuta
		SET @id_subdiario = '';
		
		SELECT tg01_subdiarios_id_c 
		INTO @id_subdiario FROM `suitecrm_administracion_2019`.tg03_comprobantes
		WHERE id = @idOrdenPago;
		SET @position =0;
		OPEN minutas_contables;
			iterator:LOOP
			BEGIN
				DECLARE exit_loop  BOOLEAN DEFAULT FALSE;
				DECLARE idProveedor 	VARCHAR(36);
				DECLARE cuit		VARCHAR(13);
				DECLARE nroCbte		VARCHAR(15);
				DECLARE importeRet  	DECIMAL(18,2);
				DECLARE recaudacion 	DECIMAL(18,2);		   	
				DECLARE comision_base	DECIMAL(18,2);
				DECLARE comision_adicional  DECIMAL(18,2);
				DECLARE ReferenciaContable VARCHAR(36);
				DECLARE CuentaInterna 	INT;
				DECLARE DebeHaber 	VARCHAR(1);
				DECLARE Concepto 	VARCHAR(100);
				DECLARE Idminuta 	INT;
				DECLARE v_centroCosto1 	VARCHAR(36);
				DECLARE Idmedio_pago 	VARCHAR(36);
				DECLARE v_total 	DECIMAL(18,2);
				DECLARE v_total_calculado	DECIMAL(18,2);		
			
				DECLARE CONTINUE HANDLER FOR NOT FOUND
				BEGIN
					SET exit_loop = TRUE;
				END;
		    
				SET v_centroCosto1 = '0';
				SET Idmedio_pago = '';
				
				/*
				opp.opp_impneto  									-> @impneto,
				opp.opp_impbruto 									-> @impbruto,
				opp_impimponible AS imponible  								-> @imponible, 
				
				COALESCE(opp.opp_ret_ley20630, 0) + COALESCE(opp.opp_ret_ley23351, 0)		 	-> @ret_gan,
				opp.opp_ret_ley11265 AS ret_cudaio, 							-> @ret_cudaio, 
				opp.opp_ret_otros, 									-> @ret_otros,
				*/
			    
				FETCH NEXT FROM minutas_contables 
				INTO ReferenciaContable,CuentaInterna,DebeHaber,Concepto,Idminuta;
					
				-- aca logueo todos los campos del cursor
				INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria 
				- ReferenciaContable: ',         ReferenciaContable,			
				' - CuentaInterna: ',              CuentaInterna,
				' - DebeHaber: ',               DebeHaber,
				' - Concepto: ',          Concepto,
				' - Idminuta: ',              Idminuta
				);
				              
					
				IF exit_loop THEN 
					LEAVE iterator;
				END IF;
		
				SET @id_imputacion = UUID();
				SET @id_mpvalor = '';
				SET @cerrado = 1;
			
				-- calculo de Idmedio_pago
				-- inserto en la cartera de valores
				IF (CuentaInterna <> 0) THEN
			
					SELECT CASE @id_producto WHEN 4 THEN p.par_idmediopago_q6br 
								 WHEN 8 THEN p.par_idmediopago_q6br 
							ELSE p.par_idmediopago_otro END
					INTO Idmedio_pago 
					FROM pre_parametros p;
					
					SET @idcuentabancaria = '';
					SET @idtipomediopago = '';
					SET @id_banco = '';
					
					SELECT id_ctabancaria, id_tipomediopago, id_banco 
					INTO @idcuentabancaria, @idtipomediopago, @id_banco 
					FROM `suitecrm_administracion_2019`.v_tg01_cajamediospago
					WHERE id_caja = @caja_santafe AND id_mediopago = Idmedio_pago;
					
					SET @id_mpvalor  = UUID();
					SET @id_mpvalor1 = @id_mpvalor;
					SET @id_imputacion1 = @id_imputacion;
					
					INSERT INTO `suitecrm_administracion_2019`.`tg05_carteradevalores`(
						 id,	NAME,  date_entered,
						 date_modified,	 deleted,
						 numerovalor,	 importevalor, fecha,
						 fechaacreditacion, tg04_imputaciones_id_c,
						 tg01_mediospago_id_c,	 account_id2_c,
						  -- ctacte, egreso, ingreso/alta
						 tg03_comprobantes_id_c, tg03_comprobantes_id1_c, tg03_comprobantes_id2_c,  
						 tg01_cajas_id_c,
						 tg01_cajas_id1_c, tg01_tipomediopago_id_c, origen, tg01_referenciascontables_id_c,
						 cuentainterna, tg01_bancos_id_c
					)
					VALUES(	 @id_mpvalor, @nliq, NOW(),
						 NOW(),	 0,
						 0, @impneto, @fechaCpte,
						 @fechaCpte,	 @id_imputacion,
						 Idmedio_pago, @idReferente,
						 '', @idOrdenPago, @idOrdenPago, 
						 @caja_santafe,
						 @idcuentabancaria,  @idtipomediopago, 'P', ReferenciaContable, CuentaInterna, @id_banco
					);
				
				END IF;
			
				IF (Concepto = 'total_impuestos' ) THEN
					IF DebeHaber = 'D' THEN
						SET @debe  =  @ret_gan+@ret_cudaio;
						SET @haber = 0;
					ELSE
						SET @haber  =  @ret_gan+@ret_cudaio;
						SET @debe = 0;
					END IF;
				END IF;
				
				IF (Concepto = 'ret_ganancias' ) THEN
					IF DebeHaber = 'D' THEN
						SET @debe  =  @ret_gan;
						SET @haber = 0;
					ELSE
						SET @haber  =  @ret_gan;
						SET @debe = 0;
					END IF;
				END IF;
			
				IF (Concepto = 'ret_cudaio' ) THEN
					IF DebeHaber = 'D' THEN
						SET @debe  =  @ret_cudaio;
						SET @haber = 0;
					ELSE
						SET @haber  =  @ret_cudaio;
						SET @debe = 0;
					END IF;
				END IF;
				
				IF (Concepto = 'neto' ) THEN
					IF DebeHaber = 'D' THEN
						SET @debe  =  @impneto;
						SET @haber = 0;
					ELSE
						SET @haber  =  @impneto;
						SET @debe = 0;
					END IF;
				END IF;
				
				IF (Concepto = 'premios' OR Concepto = 'bruto' ) THEN -- ADEN - 2024-11-14 - Se agrega el bruto
					IF DebeHaber = 'D' THEN
						SET @debe  =  @impbruto;
						SET @haber = 0;
					ELSE
						SET @haber  =  @impbruto;
						SET @debe = 0;
					END IF;
				END IF;
			
				IF (@debe <>0 OR @haber <> 0) THEN
				
					INSERT INTO `suitecrm_administracion_2019`.`tg04_imputaciones` (
						id
						, NAME
						, ordenitem
						, date_entered
						, date_modified
						, modified_user_id
						, created_by
						, description
						, deleted
						, assigned_user_id
						, `tg01_tipooperacion_id_c`
						, `origen`
						, `fecha`
						, `debe`
						, `haber`
						, `debemxd`
						, `habermxd`
						, `tiporeferente`
						, `account_id_c`
						, `tg03_comprobantes_id_c`
						, `tg03_comprobantes_id1_c`
						, `estado`
						, `cerrado`
						, tg01_centrocosto_id_c
						, tg01_referenciascontables_id_c
						, tg01_mediospago_id_c
						, tg05_carteradevalores_id_c
						, tg01_subdiarios_id_c
						, tg01_cajas_id_c
					)
					VALUES(
						@id_imputacion
						, @nliq	
						, Idminuta*10+@position
						, NOW()
						, NOW()
						, par_idUsuario
						, par_idUsuario
						, ''
						, 0
						, par_idUsuario
						, @tipooperacion_caja  
						, 'R' 
						, @fechaCpte
						, @debe   
						, @haber
						, @debe
						, @haber
						, 'OP'
						, @idReferente
						, @idOrdenPago
						, @idcomprobantepremio
						, 'B'
						, @cerrado
						, 0
						, ReferenciaContable
						, Idmedio_pago
						, @id_mpvalor
						, @id_subdiario
						, @caja_santafe
					);
						
					SET @position = @position + 1;	
						
					-- inserta la relacion
					INSERT INTO `suitecrm_administracion_2019`.`tg03_comprobantes_tg04_imputaciones_c`( `id`, date_modified, deleted, 
						`tg03_comprobantes_tg04_imputacionestg03_comprobantes_ida`,
						`tg03_comprobantes_tg04_imputacionestg04_imputaciones_idb`)
					VALUES(UUID(), NOW(),0,@idOrdenPago, @id_imputacion);	
					
					INSERT INTO `suitecrm_administracion_2019`.`tg03_comprobantes_tg04_imputaciones_c`( `id`, date_modified, deleted, 
							`tg03_comprobantes_tg04_imputacionestg03_comprobantes_ida`,
							`tg03_comprobantes_tg04_imputacionestg04_imputaciones_idb`)
					VALUES(UUID(), NOW(),0,@id_imputacion, @idOrdenPago );		
				
				END IF;
			END;
			END LOOP iterator;
		CLOSE minutas_contables;
	
		SET @Idmedio_pago = '';
		SELECT 	CASE @id_producto WHEN 4 THEN p.par_idmediopago_q6br 
					  WHEN 8 THEN p.par_idmediopago_q6br 
			ELSE p.par_idmediopago_otro END
		INTO @Idmedio_pago 
		FROM pre_parametros p;
						
		-- Genera el medio de pago asociado a la liquidacion
		SET @liq_mpago = UUID();
		
		INSERT INTO `suitecrm_administracion_2019`.`tg12_liquidacion_mediospagos`
		(`id`,`name`,`date_entered`,`created_by`,`importe`,`tg01_mediospago_id_c`, tg05_carteradevalores_id_c, tg04_imputaciones_id_c)
		VALUES(@liq_mpago, @nliq, NOW(), '1', @impneto, @Idmedio_pago, @id_mpvalor1, @id_imputacion1);
		INSERT INTO `suitecrm_administracion_2019`.`tg12_liquidacion_mediospagos_tg12_liquidacion_c`
		(`id`,`date_modified`,`tg12_liqui4421idacion_ida`,`tg12_liqui1c84ospagos_idb`)
		VALUES(UUID(), NOW(), ID_Liquidacion, @liq_mpago);	
	
	END IF;
	/* FIN MINUTAS */
END IF;
SET @etapa = @etapa + 1; -- AGREGADO ADEN 2024-11-25
IF (@idOrdenPago <> '') THEN -- ADEN - 2025-05-07 - SE CONDICIONA LOG Y UPDATE A QUE TENGA VALOR @idOrdenPago
	INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Actualizo comprobantes 1 - idOrdenPago: ' ,@idOrdenPago, ' - etapa: ',@etapa);
	-- asigna juego-sorteo al comprobante y minuta
	UPDATE suitecrm_administracion_2019.tg03_comprobantes a
		LEFT JOIN suitecrm_administracion_2019.tg04_imputaciones b ON b.tg03_comprobantes_id_c = a.id
			SET a.tg01_juegos_id_c = @id_producto, a.sorteo = @nrosorteo, b.tg01_juegos_id_c = @id_producto, b.sorteo = @nrosorteo
		WHERE a.id = @idOrdenPago;
END IF;
SET @etapa = @etapa + 1; -- AGREGADO ADEN 2024-11-25
IF (@idcomprobantepremio <> '') THEN -- ADEN - 2025-05-07 - SE CONDICIONA LOG Y UPDATE A QUE TENGA VALOR @idcomprobantepremio
	INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Actualizo comprobantes 2 - idOrdenPago: ' ,@idOrdenPago, ' - etapa: ',@etapa);
	UPDATE suitecrm_administracion_2019.tg03_comprobantes a
		LEFT JOIN suitecrm_administracion_2019.tg04_imputaciones b ON b.tg03_comprobantes_id_c = a.id
			SET a.tg01_juegos_id_c = @id_producto, a.sorteo = @nrosorteo, b.tg01_juegos_id_c = @id_producto, b.sorteo = @nrosorteo
		WHERE a.id = @idcomprobantepremio;
END IF;
INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Actualizo comprobantes f - idOrdenPago: ' ,@idOrdenPago, ' - etapa: ',@etapa);
-- 01-Fin. Referente y liquidacion si canal tesoreria, otras provincias y agencia cuando para agencia
-- 02-Inicio. Crea comprobantes de retencion
	
	/* inicio retenciones*/
	IF ( @ret_gan <> 0 ) THEN 
		
		SET @idTipoRet = '';
		SET @idTalonario = '';
		SELECT  COALESCE(a.tg01_tipocomprobante_id_c, ''), COALESCE(a.tg01_talonarios_id_c, '') 
		INTO @idTipoRet, @idTalonario
		FROM `suitecrm_administracion_2019`.tg01_numeradores a
		INNER JOIN `suitecrm_administracion_2019`.tg01_tipocomprobante b ON b.id = a.tg01_tipocomprobante_id_c AND b.idtipocomp = 'RET'
		INNER JOIN `suitecrm_administracion_2019`.tg01_talonarios c ON c.id = a.tg01_talonarios_id_c AND c.talonario = 'RETG';
			
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - COMPRB DE RETENCION - @fecha: ', @fecha);
			
		-- insertar cabecera de comprobantes (SP_ET_CabeceraINS) comprobante de retencion 		
		CALL `suitecrm_administracion_2019`.SP_ET_CabeceraINS (
			  @idTipoRet     	-- se recupera id de la consulta?          
			, NULL			-- PModelocbte             
			, '' -- @nliq -- '' -- nroCbte 		-- PNroCbte se recupera nro_retencion de la consulta?             
			, @ret_gan		-- ley20630+ley23351  
			, @fechaCpte	-- PFechacbte se recupera y se arma del select? @fechaCursor           
			, @idReferente         	-- PidProveedor se recupera del select (idProveedor )?      
			, NULL 			-- PCAE                    
			, NULL			-- PFormaPago              
			, NULL 			-- PidcondComercializacion 
			, '' 					-- PIdSucursal              -- si no seenvia toda de parametros
			, ''				 	-- PIdDeposito              -- si no seenvia toda de parametros
			, '' 					-- PIdLista                 -- si no seenvia toda de parametros
			, '' 					-- PIdMoneda                -- si no seenvia toda de parametros
			, NULL 					-- PCotizacion             
			, @fechaCpte	 			-- PFechaBase   @fechaCursor              
			, @fechaCpte 			-- PFechaContable  @fechaCursor            
			, '' 					-- Pidcaja                  -- si no seenvia toda de parametros
			, par_idUsuario		   		-- PidUsuario es el parametro de entrada?              
			, CONCAT('Retención Orden Pago: ',@nliq)   	-- PObservaciones  @obs        
			, @estado_comp					-- EstadoComprobante       
			, NULL					-- ID_Expediente           
			, @idTalonario         			-- id_talonario            
			, NULL 					-- id_remesa               
			, ID_Liquidacion			-- id_liquidacion 
			, NULL                                  -- PDA      
			, @RCode
			, @RTxt
			, @RId
			, @RSQLErrNo
			, @RSQLErrtxt
		);
			
		-- Errores
		IF (@RCode <> 1) THEN
			ROLLBACK;
			
			SET 	RCode      = @RCode,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;
			
			LEAVE thisSP ;
		END IF;
			
		SET @idComprbRetencion = @RId;
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin CALL SP_ET_CabeceraINS - COMPRB DE RETENCION: ',@idComprbRetencion);
		
		-- establecer la relacion de ambos comprobantes en tg03_comprobantes_tg03_comprobantes_c
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio tg03_comprobantes_tg03_comprobantes_c Id1 - idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
		
		 -- insert tabla relacion 
		SET @Id1 = UUID(); -- id
		INSERT INTO `suitecrm_administracion_2019`.tg03_comprobantes_tg03_comprobantes_c (
			id
			, date_modified
			, deleted
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_ida` -- idOrdenPago
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_idb` -- idComprbRetencion
		)
		VALUES(
			@Id1
			, NOW()
			, 0
			, @idOrdenPago -- idOrdenPago
			, @idComprbRetencion -- idComprbRetencion
		); 
		    
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin tg03_comprobantes_tg03_comprobantes_c Id1 :',@Id1, 'idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio tg03_comprobantes_tg03_comprobantes_c Id12 - idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
		
		SET @Id2 = UUID(); -- id
		INSERT INTO `suitecrm_administracion_2019`.tg03_comprobantes_tg03_comprobantes_c (
			id
			, date_modified
			, deleted
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_ida` -- idComprbRetencion
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_idb` -- idOrdenPago
		)
		VALUES(
			@Id2
			, NOW()
			, 0
			, @idComprbRetencion -- idComprbRetencion
			, @idOrdenPago -- idOrdenPago
		);
			
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin tg03_comprobantes_tg03_comprobantes_c Id2 :',@Id2, 'idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
			
		-- llamar sp de retenciones-percepciones ins v2
			
		-- DATOS DE RETENCIONES - referenciaContable - modeloimpuestoib - impuestoib 			
		SET @referenciaContable = '';
		SET @modeloimpuestoib = '';
		SET @impuestoib = '';
		SET @alicuota = '';
		SELECT `par_id_referenciacontableg`,`par_id_modeloimpuestog`,`par_id_impuestog`, `par_alicuotaG`
		INTO @referenciaContable, @modeloimpuestoib, @impuestoib, @alicuota
		FROM pre_parametros_ganancias PP
		WHERE PP.deleted = 0 AND PP.sor_producto_id_c = @id_producto;	
						
		-- recupera name de la orden de pago
		SET @nroOrdenPago = '';
		SELECT NAME 
		INTO @nroOrdenPago
		FROM `suitecrm_administracion_2019`.tg03_comprobantes
		WHERE id = @idOrdenPago; 
					
		-- recupera name del comprobante de retencion
		SET @nroretencion = '';
		SELECT NAME 
		INTO @nroretencion
		FROM `suitecrm_administracion_2019`.tg03_comprobantes
		WHERE id = @idComprbRetencion; 
		
		-- determina el id de comprobante que pasa para generar el detalle de la retencion
		IF (@canal_pago = 'T') THEN 
			SET @id_comp_retencion = @idcomprobantepremio;
		ELSE
			SET @id_comp_retencion = @idOrdenPago;
		END IF;
		CALL `suitecrm_administracion_2019`.`SP_ET_RetencionesPercepcionesINS_V2` (			
			@nroretencion,  	-- nroCbte		-- nroFacturaPer - cursor
			@id_comp_retencion, -- @idComprbRetencion, ID_OrdenPago, -- idComprobante - SP_ET_CabeceraINS - comprb de retencion
			@impbruto,  		-- recaudacion           -- importe - cursor
			@imponible, 		-- @base        -- base - calculo en base campos cursor
			@ret_gan, 		-- importeRet            -- importeRetenido - cursor
			@alicuota, -- AlicuotaG,  
			@modeloimpuestoib, -- ID_ModeloImpuestoG,
			@impuestoib, -- ID_ImpuestoG,
			NULL,
			@referenciaContable, -- ID_ReferneciaContableG,
			RIGHT(@nroretencion,8),    -- nro certificado
			@nroOrdenPago,
			@fechaCpte, 
			@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt
		);
				
		-- Errores
		IF (@RCode <> 1) THEN
			ROLLBACK;
			SET 	RCode      = @RCode,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;		  
		  
			LEAVE thisSP ;
		END IF;
					
		SET @idRetPercep = @RId;
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Fin CALL `SP_ET_RetencionesPercepcionesINS_V2` ',@idRetPercep, '******************');
		
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Update COMPRB DE RETENCION ', 'idRetencion :',@idComprbRetencion, 'idRetencionesPercepciones :',@idRetPercep, '******************');
		
		UPDATE `suitecrm_administracion_2019`.`tg03_comprobantes`
		SET `tg05_retenciones_y_precepciones_id_c` = @idRetPercep
		WHERE id = @idComprbRetencion;
		
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Fin - Update COMPRB DE RETENCION - ******************');
		-- fin importe orden de pago en 0		
	END IF;	
		
	IF ( @ret_cudaio <> 0 ) THEN
	
		SET @idTipoRet = '';
		SET @idTalonario = '';
		SELECT  COALESCE(a.tg01_tipocomprobante_id_c, ''), COALESCE(a.tg01_talonarios_id_c, '') 
		INTO @idTipoRet, @idTalonario
		FROM `suitecrm_administracion_2019`.tg01_numeradores a
		INNER JOIN `suitecrm_administracion_2019`.tg01_tipocomprobante b ON b.id = a.tg01_tipocomprobante_id_c AND b.idtipocomp = 'RET'
		INNER JOIN `suitecrm_administracion_2019`.tg01_talonarios c ON c.id = a.tg01_talonarios_id_c AND c.talonario = 'RETV';
		
		CALL `suitecrm_administracion_2019`.SP_ET_CabeceraINS (
			  @idTipoRet     	-- se recupera id de la consulta?    misma para cudaio?       
			, NULL			-- PModelocbte             
			, '' -- @nliq -- '' -- nroCbte 		-- PNroCbte se recupera nro_retencion de la consulta?             
			, @ret_cudaio		-- ley20630+ley23351  
			, @fechaCpte		-- PFechacbte se recupera y se arma del select? @fechaCursor           
			, @idReferente         	-- PidProveedor se recupera del select (idProveedor )?      
			, NULL 			-- PCAE                    
			, NULL			-- PFormaPago              
			, NULL 			-- PidcondComercializacion 
			, '' 					-- PIdSucursal              -- si no seenvia toda de parametros
			, ''				 	-- PIdDeposito              -- si no seenvia toda de parametros
			, '' 					-- PIdLista                 -- si no seenvia toda de parametros
			, '' 					-- PIdMoneda                -- si no seenvia toda de parametros
			, NULL 					-- PCotizacion             
			, @fechaCpte	 			-- PFechaBase   @fechaCursor              
			, @fechaCpte 			-- PFechaContable  @fechaCursor            
			, '' 					-- Pidcaja                  -- si no seenvia toda de parametros
			, par_idUsuario		   		-- PidUsuario es el parametro de entrada?              
			, CONCAT('Retención CUDAIO Orden Pago: ',@nliq)   	-- PObservaciones  @obs        
			, @estado_comp					-- EstadoComprobante       
			, NULL					-- ID_Expediente           
			, @idTalonario         			-- id_talonario            
			, NULL 					-- id_remesa               
			, ID_Liquidacion			-- id_liquidacion 
			, NULL                                  -- PDA      
			, @RCode
			, @RTxt
			, @RId
			, @RSQLErrNo
			, @RSQLErrtxt
		);
			
		-- Errores
		IF (@RCode <> 1) THEN
			ROLLBACK;
			
			SET 	RCode      = @RCode,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;
			
			LEAVE thisSP ;
		END IF;
		
		SET @idComprbRetencion = @RId;
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin CALL SP_ET_CabeceraINS - COMPRB DE RETENCION: ',@idComprbRetencion);
		
		-- establecer la relacion de ambos comprobantes en tg03_comprobantes_tg03_comprobantes_c
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio tg03_comprobantes_tg03_comprobantes_c Id1 - idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
		
		 -- insert tabla relacion 
		SET @Id1 = UUID(); -- id
		INSERT INTO `suitecrm_administracion_2019`.tg03_comprobantes_tg03_comprobantes_c (
			id
			, date_modified
			, deleted
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_ida` -- idOrdenPago
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_idb` -- idComprbRetencion
		)
		VALUES(
			@Id1
			, NOW()
			, 0
			, @idOrdenPago -- idOrdenPago
			, @idComprbRetencion -- idComprbRetencion
		);
		    
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin tg03_comprobantes_tg03_comprobantes_c Id1 :',@Id1, 'idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Inicio tg03_comprobantes_tg03_comprobantes_c Id12 - idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
		
		SET @Id2 = UUID(); -- id
		INSERT INTO `suitecrm_administracion_2019`.tg03_comprobantes_tg03_comprobantes_c (
			id
			, date_modified
			, deleted
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_ida` -- idComprbRetencion
			, `tg03_comprobantes_tg03_comprobantestg03_comprobantes_idb` -- idOrdenPago
		)
		VALUES(
			@Id2
			, NOW()
			, 0
			, @idComprbRetencion -- idComprbRetencion
			, @idOrdenPago -- idOrdenPago
		);
			
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), ' - PREMIOS_conexion_tesoreria - Fin tg03_comprobantes_tg03_comprobantes_c Id2 :',@Id2, 'idOrdenPago: ' ,@idOrdenPago, 'idComprbRetencion: ',@idComprbRetencion);
			
		-- llamar sp de retenciones-percepciones ins v2
			
		-- DATOS DE RETENCIONES - referenciaContable - modeloimpuestoib - impuestoib 			
		SET @referenciaContable = '';
		SET @modeloimpuestoib = '';
		SET @impuestoib = '';
		SET @alicuota = '';
		SELECT `par_id_referenciacontablecu`,`par_id_modeloimpuestocu`,`par_id_impuestocu`, `par_alicuotaCU`
		INTO @referenciaContable, @modeloimpuestoib, @impuestoib, @alicuota
		FROM pre_parametros PP
		WHERE PP.deleted = 0;	
				
		-- recupera name de la orden de pago
		SET @nroOrdenPago = '';
		SELECT NAME 
		INTO @nroOrdenPago
		FROM `suitecrm_administracion_2019`.tg03_comprobantes
		WHERE id = @idOrdenPago; 
					
		-- recupera name del comprobante de retencion
		SET @nroretencion = '';
		SELECT NAME 
		INTO @nroretencion
		FROM `suitecrm_administracion_2019`.tg03_comprobantes
		WHERE id = @idComprbRetencion; 
		
		-- determina el id de comprobante que pasa para generar el detalle de la retencion
		IF (@canal_pago = 'T') THEN 
			SET @id_comp_retencion = @idcomprobantepremio;
		ELSE
			SET @id_comp_retencion = @idOrdenPago;
		END IF;
		CALL `suitecrm_administracion_2019`.`SP_ET_RetencionesPercepcionesINS_V2` (						
			@nroretencion,  	-- nroCbte		-- nroFacturaPer - cursor
			@id_comp_retencion,     -- @idComprbRetencion, ID_OrdenPago, -- idComprobante - SP_ET_CabeceraINS - comprb de retencion
			@impbruto,  		-- recaudacion           -- importe - cursor
			@imponible, 		-- @base        -- base - calculo en base campos cursor
			@ret_cudaio, 		-- importeRet            -- importeRetenido - cursor
			@alicuota, -- AlicuotaG,  
			@modeloimpuestoib, -- ID_ModeloImpuestoG,
			@impuestoib, -- ID_ImpuestoG,
			NULL,
			@referenciaContable, -- ID_ReferneciaContableG,
			RIGHT(@nroretencion,8),    -- nro certificado
			@nroOrdenPago,
			@fechaCpte, 
			@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt
		);
			
		-- Errores
		IF (@RCode <> 1) THEN
			ROLLBACK;
			
			SET 	RCode      = @RCode,
				RTxt       = @RTxt,
				RId        = '',
				RSQLErrNo  = @RSQLErrNo,
				RSQLErrtxt = @RSQLErrtxt;
			
			LEAVE thisSP ;
		END IF;
				
		SET @idRetPercep = @RId;
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Fin CALL `SP_ET_RetencionesPercepcionesINS_V2` ',@idRetPercep, '******************');
		
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Update COMPRB DE RETENCION ', 'idRetencion :',@idComprbRetencion, 'idRetencionesPercepciones :',@idRetPercep, '******************');
		
		UPDATE `suitecrm_administracion_2019`.`tg03_comprobantes`
		SET `tg05_retenciones_y_precepciones_id_c` = @idRetPercep
		WHERE id = @idComprbRetencion;
		
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Fin - Update COMPRB DE RETENCION - ******************');
		-- fin importe orden de pago en 0
	END IF;
	
-- 02-Fin. Crea comprobantes de retencion
	
	
-- si fue por canal tesoreria y tiene que paga en agencia
-- genera debito en cuenta corriente, para que el agente devuelva el credito del premio que no pago (solo si los premios del juego tienen modo ACREDITACION PREVIA)
-- tambien cuando es doble chance y el canal de pago es agencia
-- ADEN - 2024-11-07 - otra vez SOPA, si pagaba agencia y pago tesoreria, o si paga tesoreria x ctacte (4)
-- ADEN - 2024-12-30 - Excepcion para los juegos que no son de @modo_acred_premios = 'A' no generen el DEBITO
	IF ((@pagaagencia = 'S' AND @canal_pago = 'T' AND @modo_acred_premios = 'A') 
		  OR @forma_pago_solicitada = 4) THEN
--	          OR ((@canal_pago IN ('A', 'T') AND @id_juego  = 41)) THEN
	          
		INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria -  IF pagaagencia: ',@pagaagencia, ' - Etapa: ', @etapa, '******************');
		
		-- Actualiza el premio como pagado
		
		SET @etapa = @etapa + 1; -- AGREGADO ADEN 2024-11-25
		UPDATE pre_premios  pr
		INNER JOIN pre_orden_pago opp ON opp.pre_premios_id_c = pr.id AND opp.deleted = 0
		SET pr.pre_estadopago = 'A' 
			, opp.modified_user_id =  par_idUsuario
			, opp.date_modified = CONCAT(CURDATE(), ' 11:00:00') 
		WHERE pr.deleted = 0 AND opp.id = opp_id;
		
		-- 2-9-2024 -lmariotti actualiza estado del documento a confirmado. Los de tesoreria, se hacen al confirmar la liquidacion
	
		SET @etapa = @etapa + 1; -- AGREGADO ADEN 2024-11-25
		
		UPDATE pre_orden_pago a
		INNER JOIN pre_orden_pago_documentos b ON  a.id = b.pre_orden_pago_id_c AND b.deleted = 0
		SET b.opd_estado = 'C'
		WHERE a.id = opp_id;
		
		-- determina estado de la prescripcion del premio
		-- si esta prescripto, no genera ND para el agente, dado que ya lo hizo el proceso de prescripcion
		
		SET @etapa = @etapa + 1; -- AGREGADO ADEN 2024-11-25
		SELECT  COALESCE(c.sor_presc_recibida,0)  INTO  @sor_presc_recibida
		FROM pre_orden_pago a
		INNER JOIN pre_premios b ON b.id = a.pre_premios_id_c
		INNER JOIN sor_pgmsorteo c ON c.id = b.sor_pgmsorteo_id_c
		WHERE a.id = opp_id;
		
		-- si el sorteo esta prescripto, no genera la ND, porque ya se la cobraron en el proceso de prescripcion de BOLDT
		
		IF @sor_presc_recibida = 0 THEN
	
			-- recupera parametros de cuenta corriente. cod tipo mov. y cod afect. + idOperacion AGENTES - DB-CR VARIOS -- ADEN 2024-11-08 - AGREGO tk
			SET @tmov_ajdb = 0, @afect_ajdb = 0, @idOperacion = '',  @tmov_ajdb_doblec = '', @afect_ajdb_doblec = '', @tmov_ajdb_tk = '',  @afect_ajdb_tk = '';
			SELECT  tmov_ajdb,  afect_ajdb,  id_op_deb_age,  tmov_ajdb_doblec,  afect_ajdb_doblec, tmov_ajdb_tk,  afect_ajdb_tk
			INTO   @tmov_ajdb, @afect_ajdb, @idOperacion,   @tmov_ajdb_doblec, @afect_ajdb_doblec, @tmov_ajdb_tk,  @afect_ajdb_tk
			FROM cas02_cc_parametros;
						
			-- doble chance, cambia el comprobante-afectacion
			IF @id_juego  = 41 THEN
				SET  @tmov_ajdb  = @tmov_ajdb_doblec;
				SET  @afect_ajdb = @afect_ajdb_doblec;
			END IF;
			-- telekino x ctacte, cambia el comprobante-afectacion - ADEN - 2024-11-08
			IF (@id_juego  = 73 AND @forma_pago_solicitada = 4) THEN
				SET  @tmov_ajdb  = @tmov_ajdb_tk;
				SET  @afect_ajdb = @afect_ajdb_tk;
			END IF;
			-- recupera idAgeRed, nombreRed
			SET @idAgeRed = '', @nombreRed = '', @importeLiquidado = 0;
			
			SELECT ap.`age_red_id_c`, ar.nombre_red, opp_impneto
			INTO @idAgeRed, @nombreRed, @importeLiquidado    
			FROM pre_orden_pago opp
			    INNER JOIN pre_premios b ON b.id = opp.pre_premios_id_c
			    INNER JOIN accounts ac ON ac.id = b.account_id_c
			    INNER JOIN `accounts_cstm` accm ON accm.id_c = ac.id
			    INNER JOIN `age_permiso` ap ON ap.id_permiso = accm.id_permiso_c
			    INNER JOIN age_red ar ON ar.id = ap.age_red_id_c
			WHERE opp.id = opp_id ;
			-- set delegacion, grupo, fecha_carga
			SET @delegacion = '0', @grupo = 'A', @fecha_carga = CURDATE() ;
			
			-- datos para movimientos
			SET @idTipoCbte = '', @idAfectacion = '', @fecha_ingreso = CURDATE(), @tipoDC = '';
			-- id tipo cbte, tipo_dc
			SELECT id, tipo_dc 
			INTO @idTipoCbte, @tipoDC
			FROM cas02_cc_tipos_movimientos
			WHERE codigo = @tmov_ajdb AND grupo = @grupo;
			-- id afectacion 
			SELECT id 
			INTO @idAfectacion
			FROM cas02_cc_afectaciones
			WHERE codigo_de_afectacion = @afect_ajdb AND grupo = @grupo;	
			-- llamar SP_ET_ManualesINS
			INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Inicio CALL SP_ET_ManualesINS ******************');
			CALL `SP_ET_ManualesINS` (						
					par_idUsuario,  	
					@idOperacion,
					@idAgeRed,  	
					@delegacion, 		-- 0 = Santa fe
					@grupo, 		
					@fecha_carga, 
					'', 
					@importeLiquidado,  -- es el imp neto?
					0, 	
					@nombreRed,	
					'',                 -- fecha valores
					@tipoDC,            -- tipo_dc
					@idSorteo,          -- agregado	
					0,		    -- agregado	
					0,
					@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt
			);
			
			-- Errores
			IF (@RCode <> 1) THEN
				ROLLBACK;
				SET 	RCode      = @RCode,
					RTxt       = @RTxt,
					RId        = '',
					RSQLErrNo  = @RSQLErrNo,
					RSQLErrtxt = @RSQLErrtxt;
			  
				LEAVE thisSP ;
			END IF;
						
			SET @idManual = @RId;
			SET idLiquidacion = NULL;
			INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Fin CALL `SP_ET_ManualesINS` ',@idManual, ' - etapa: ', @etapa, '******************');
			-- llamar SP_ET_MovimientosINS
			INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Inicio CALL SP_ET_MovimientosINS - etapa: ', @etapa, '******************');	
			CALL `SP_ET_MovimientosINS` (						
				@idManual,  	
				idLiquidacion,
				par_idUsuario,  	
				@idTipoCbte,   
				@idAfectacion, 		
				@idAgeRed, 				
				@idTipoCbte, 			
				@tmov_ajdb,	
				@importeLiquidado,  -- es el imp neto?		
				@importeLiquidado,  -- importeLiquidado = saldo?	
				@tipoDC,
				@fecha_ingreso,
				@delegacion,
				@grupo,
				0, -- intereses
				@idSorteo,          -- agregado	
				@id_provincia,      -- agregado
				@RCode, @RTxt, @RId, @RSQLErrNo, @RSQLErrtxt
			);
			
			-- Errores
			IF (@RCode <> 1) THEN
				ROLLBACK;
				
				SET 	RCode      = @RCode,
					RTxt       = @RTxt,
					RId        = '',
					RSQLErrNo  = @RSQLErrNo,
					RSQLErrtxt = @RSQLErrtxt;
			  
				LEAVE thisSP ;
			END IF;
					
			SET @idMovimiento = @RId;
			INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Fin CALL `SP_ET_MovimientosINS` ',@idMovimiento, '******************');
		ELSE
			INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - ID Premio no genera ND en cta cte. esta prescripto el sorteo` ',opp_id);
		END IF;
	END IF;
	
UPDATE  pre_orden_pago opp
SET 	opp.estado_envio_as400 = 'E',
	opp.date_modified = NOW()
WHERE opp.id = opp_id;
INSERT INTO kk_auditoria(nombre) SELECT CONCAT(NOW(), 'PREMIOS_conexion_tesoreria - Actualiza Orde de Pago: ', opp_id, ' - etapa: ', @etapa);
	
COMMIT;
	
SET  RId = ID_Liquidacion;
	
END$$

DELIMITER ;