DELIMITER $$

USE `suitecrm_cas`$$

DROP PROCEDURE IF EXISTS `PREMIOS_uif_act_personas`$$

CREATE DEFINER=`sp_funciones`@`localhost` PROCEDURE `PREMIOS_uif_act_personas`(OUT msgret VARCHAR(255))
fin: BEGIN
 	DECLARE finished INT DEFAULT 0;
 	DECLARE cnt_altas INT DEFAULT 0;
 	DECLARE cnt_altas_ant INT DEFAULT 0;
 	DECLARE cnt_modif INT DEFAULT 0;
 	DECLARE cnt_modif_ant INT DEFAULT 0;
	DECLARE alta, modif BOOLEAN;
 	
        DECLARE bene_cuit_x, bene_cuit_a, bene_cuit_n VARCHAR(11) DEFAULT NULL;
        
        DECLARE bene_tipo_x, bene_doc_tipo_x, bene_sexo_x, bene_estado_civil_x, bene_pep_x, 
		bene_tipo_a, bene_doc_tipo_a, bene_sexo_a, bene_estado_civil_a, bene_pep_a, 
		bene_tipo_n, bene_doc_tipo_n, bene_sexo_n, bene_estado_civil_n, bene_pep_n VARCHAR(100) DEFAULT NULL
		; 
		
        DECLARE bene_nombre_x, bene_apemat_x, bene_domi_x, bene_cargo_x, bene_telefono_x, bene_email_x, 
		bene_nombre_a, bene_apemat_a, bene_domi_a, bene_cargo_a, bene_telefono_a, bene_email_a, 
		bene_nombre_n, bene_apemat_n, bene_domi_n, bene_cargo_n, bene_telefono_n, bene_email_n VARCHAR(255) DEFAULT NULL
		
		; 
		
        DECLARE bene_doc_nro_x, bene_doc_nro_a, bene_doc_nro_n INT(11) DEFAULT NULL;
        
        DECLARE tbl_localidades_id_c_x, tbl_provincias_id_c_x, tbl_nacionalidades_id_c_x, tbl_ocupaciones_id_c_x,
		tbl_localidades_id_c_a, tbl_provincias_id_c_a, tbl_nacionalidades_id_c_a, tbl_ocupaciones_id_c_a,
		tbl_localidades_id_c_n, tbl_provincias_id_c_n, tbl_nacionalidades_id_c_n, tbl_ocupaciones_id_c_n CHAR(36) DEFAULT NULL;
        DECLARE bene_fechanac_x, bene_fechanac_a, bene_fechanac_n DATE DEFAULT NULL;
        
	-- agrego declares para nuevos campos:
        DECLARE bene_telefono_nro_x,bene_telefono_nro_a, bene_telefono_nro_n VARCHAR(9) DEFAULT ''; 	
        
        DECLARE	bene_telefono_carac_x, bene_telefono_carac_a, bene_telefono_carac_n VARCHAR(5) DEFAULT ''; 
        
	DECLARE bene_domi_calle_x, bene_snombre_x, bene_ape_x, bene_nombre2_x,
	        bene_domi_calle_a, bene_snombre_a, bene_ape_a, bene_nombre2_a,
	        bene_domi_calle_n, bene_snombre_n, bene_ape_n, bene_nombre2_n VARCHAR(255) DEFAULT '';
	        
	DECLARE bene_domi_nro_x, bene_domi_piso_x, bene_domi_dpto_x,
		bene_domi_nro_a, bene_domi_piso_a, bene_domi_dpto_a, 
		bene_domi_nro_n, bene_domi_piso_n, bene_domi_dpto_n VARCHAR(10) DEFAULT '';
		
	DECLARE bene_tipo_soc_x, bene_tdoc_afip_x, bene_actividad_x, 
	bene_tipo_soc_a, bene_tdoc_afip_a, bene_actividad_a, 
	bene_tipo_soc_n, bene_tdoc_afip_n, bene_actividad_n VARCHAR(100);
	
	DECLARE	bene_fescritura_x,
		bene_fescritura_a,
		bene_fescritura_n DATE DEFAULT NULL;
	DECLARE	tbl_paises_id_c_x, 
		tbl_paises_id_c_a,
		tbl_paises_id_c_n CHAR(36);
	-- FIN agrego declares para nuevos campos
	
	-- ini
	DECLARE pers CURSOR 
		FOR SELECT -- los _x... los con * son los campos que se agregan...
			opb.opb_cuit AS per_cuit,     -- bene_cuit_x
			opb.opb_tipo AS per_tipo, -- bene_tipo_x
			COALESCE(opb.opb_nombre, '') AS per_nombre, -- bene_nombre_x
			COALESCE(opb.opb_apemat, '') AS per_apemat, -- bene_apemat_x
			COALESCE(opb.opb_tdoc, '') AS per_doc_tipo, -- bene_doc_tipo_x
			COALESCE(opb.opb_doc_nro, 0) AS per_doc_nro, -- bene_doc_nro_x
			COALESCE(opb.opb_domi, '') AS per_domi, -- bene_domi_x
			COALESCE(opb.tbl_localidades_id_c, '') AS tbl_localidades_id_c, -- tbl_localidades_id_c_x
			COALESCE(opb.tbl_provincias_id_c, '') AS tbl_provincias_id_c, -- tbl_provincias_id_c_x
			COALESCE(opb.opb_sexo, '') AS per_sexo, -- bene_sexo_x
			COALESCE(opb.opb_estado_civil, '') AS per_estado_civil, -- bene_estado_civil_x
			COALESCE(opb.tbl_nacionalidades_id_c, '') AS tbl_nacionalidades_id_c, -- tbl_nacionalidades_id_c_x
			COALESCE(opb.opb_cargo, '') AS per_cargo, -- bene_cargo_x
			COALESCE(opb.opb_pep, 'N') AS per_pep, -- bene_pep_x
			opb.opb_fechanac AS per_fechanac, -- bene_fecha_nac_x
			COALESCE(opb.tbl_ocupaciones_id_c, '') AS tbl_ocupaciones_id_c, -- tbl_ocupaciones_id_c_x
			TRIM(CONCAT(TRIM(COALESCE(opb.opb_telefono_carac,'')), ' ', TRIM(COALESCE(opb.opb_telefono_numero,'')))) AS per_telefono, -- bene_telefono_x
			COALESCE(opb.opb_email, '') AS per_email, -- bene_email_x
			TRIM(COALESCE(opb.opb_telefono_numero,'')) AS per_telefono_numero, -- *bene_telefono_nro_x
			TRIM(COALESCE(opb.opb_telefono_carac,'')) AS per_telefono_carac, -- *bene_telefono_carac_x
			TRIM(COALESCE(opb.opb_domi_calle, '')) AS per_domi_calle, -- *bene_domi_calle_x
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_nro, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_nro)
			END AS per_domi_nro, -- *bene_domi_nro_x
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_piso, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_piso)
			END AS per_domi_piso, -- *bene_domi_piso_x
			CASE 
				WHEN TRIM(COALESCE(opb.opb_domi_dpto, '0')) = '0' 	
					THEN ''
					ELSE TRIM(opb.opb_domi_dpto)
			END AS per_domi_dpto, -- *bene_domi_dpto_x
			COALESCE(opb.opb_tipo_soc, '25') AS per_tipo_soc, -- *bene_tipo_soc_x -- 25 -> "otros"
			'' AS per_actividad, -- *bene_actividad_x
			NULL AS per_fescritura, -- *bene_fescritura_x
			TRIM(COALESCE(opb.opb_snombre, '')) AS per_snombre, -- *bene_snombre_x
			TRIM(COALESCE(opb.opb_apellido, '')) AS per_apellido, -- *bene_ape_x
			COALESCE(opb.tbl_paises_id_c, '') AS tbl_paises_id_c, -- *tbl_paises_id_c_x
			COALESCE(opb.tipo_documental_afip, '') AS per_tipo_documental_afip, -- *bene_tdoc_afip_x
			TRIM(COALESCE(opb.opb_snombre, '')) AS per_nombre2 -- *bene_nombre2_x
			,
			-- los _a... los con * son los campos que se agregan...
			p.per_cuit AS a_per_cuit,     -- bene_cuit_a
			p.per_tipo AS a_per_tipo, -- bene_tipo_a
			COALESCE(p.per_nombre, '') AS a_per_nombre, -- bene_nombre_a
			COALESCE(p.per_apemat, '') AS a_per_apemat, -- bene_apemat_a
			COALESCE(p.per_doc_tipo, '') AS a_per_doc_tipo, -- bene_doc_tipo_a
			COALESCE(p.per_doc_nro, 0) AS a_per_doc_nro, -- bene_doc_nro_a
			COALESCE(p.per_domi, '') AS a_per_domi, -- bene_domi_a
			COALESCE(p.tbl_localidades_id_c, '') AS a_tbl_localidades_id_c, -- tbl_localidades_id_c_a
			COALESCE(p.tbl_provincias_id_c, '') AS a_tbl_provincias_id_c, -- tbl_provincias_id_c_a
			COALESCE(p.per_sexo, '') AS a_per_sexo, -- bene_sexo_a
			COALESCE(p.per_estado_civil, '') AS a_per_estado_civil, -- bene_estado_civil_a
			COALESCE(p.tbl_nacionalidades_id_c, '') AS a_tbl_nacionalidades_id_c, -- tbl_nacionalidades_id_c_a
			COALESCE(p.per_cargo, '') AS a_per_cargo, -- bene_cargo_a
			COALESCE(p.per_pep, 'N') AS a_per_pep, -- bene_pep_a
			p.per_fechanac AS a_per_fechanac, -- bene_fecha_nac_a
			COALESCE(p.tbl_ocupaciones_id_c, '') AS a_tbl_ocupaciones_id_c, -- tbl_ocupaciones_id_c_a
			TRIM(CONCAT(TRIM(COALESCE(p.per_telefono_carac,'')), ' ', TRIM(COALESCE(p.per_telefono_numero,'')))) AS a_per_telefono, -- bene_telefono_a
			COALESCE(p.per_email, '') AS a_per_email, -- bene_email_a
			TRIM(COALESCE(p.per_telefono_numero,'')) AS a_per_telefono_numero, -- *bene_telefono_nro_a
			TRIM(COALESCE(p.per_telefono_carac,'')) AS a_per_telefono_carac, -- *bene_telefono_carac_a
			TRIM(COALESCE(p.per_domi_calle, '')) AS a_per_domi_calle, -- *bene_domi_calle_a
			CASE 
				WHEN TRIM(COALESCE(p.per_domi_nro, '0')) = '0' 	
					THEN ''
					ELSE TRIM(p.per_domi_nro)
			END AS a_per_domi_nro, -- *bene_domi_nro_a
			CASE 
				WHEN TRIM(COALESCE(p.per_domi_piso, '0')) = '0' 	
					THEN ''
					ELSE TRIM(p.per_domi_piso)
			END AS a_per_domi_piso, -- *bene_domi_piso_a
			CASE 
				WHEN TRIM(COALESCE(p.per_domi_dpto, '0')) = '0' 	
					THEN ''
					ELSE TRIM(p.per_domi_dpto)
			END AS a_per_domi_dpto, -- *bene_domi_dpto_a
			COALESCE(p.per_tipo_soc, '') AS a_per_tipo_soc, -- *bene_tipo_soc_a 
			COALESCE(p.per_actividad,'') AS a_per_actividad, -- *bene_actividad_a
			p.per_fescritura AS a_per_fescritura, -- *bene_fescritura_a
			TRIM(COALESCE(p.per_snombre, '')) AS a_per_snombre, -- *bene_snombre_a
			TRIM(COALESCE(p.per_apellido, '')) AS a_per_apellido, -- *bene_ape_a
			COALESCE(p.tbl_paises_id_c, '') AS a_tbl_paises_id_c, -- *bene_domi_pais_a
			COALESCE(p.per_tipo_documental_afip, '') AS a_per_tipo_documental_afip, -- *bene_tdoc_afip_a
			TRIM(COALESCE(p.per_snombre, '')) AS a_per_nombre2 -- *bene_nombre2_a
		
		FROM pre_orden_pago op 
			JOIN pre_premios pp ON pp.id = op.pre_premios_id_c AND pp.deleted = 0 AND COALESCE(op.pre_premios_cuotas_id_c,'') = '' 
			JOIN pre_orden_pago_beneficiarios opb ON opb.pre_orden_pago_id_c = op.id AND opb.deleted = 0 -- AND opb.opb_tipobeneficiario = 'C' 
			JOIN pre_tipo_comprobante tc ON tc.id = op.pre_tipo_comprobante_id_c AND tc.deleted = 0
			LEFT JOIN uif_persona p ON p.per_cuit = opb.opb_cuit AND p.deleted = 0 -- para actualizar las que ya estan
		WHERE op.opp_estado_registracion = 'D' 
		  AND COALESCE(op.pre_premios_cuotas_id_c, '') = '' AND op.deleted = 0
		  AND opb.opb_cuit IS NOT NULL
		  AND opb.opb_cuit NOT IN (SELECT cuit FROM cuit_paises_v)
		  AND op.opp_fecha_comprobante >= DATE_SUB(CURDATE(),INTERVAL 7 DAY)
		  -- AND (op.opp_estado_actualizacion_fpag <> 'P' OR pp.pre_estadopago ='E') -- asterisqueado porque no me interesan los datos de pago, alcanza con que la carga del beneficiario sea "Definitiva"
		ORDER BY opb.opb_cuit, op.opp_fecha_comprobante, op.opp_fecha_pago
	;
	-- fin
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1
			@code = RETURNED_SQLSTATE, @msg = MESSAGE_TEXT, @errno = MYSQL_ERRNO, 
			@base = SCHEMA_NAME, @tabla = TABLE_NAME; -- estas no las recupera???
		INSERT INTO kk_auditoria(nombre) VALUES(CONCAT(NOW(), ' - PREMIOS_uif_act_personas - Error ', COALESCE(@errno, 0), ' Mensaje ', COALESCE(@msg, '')));	
		SET msgret = 'Error';
	END;
                        
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
	OPEN pers;
	FETCH pers INTO bene_cuit_x, bene_tipo_x, bene_nombre_x, bene_apemat_x, bene_doc_tipo_x, bene_doc_nro_x, 
			bene_domi_x, tbl_localidades_id_c_x, tbl_provincias_id_c_x, 
			bene_sexo_x, bene_estado_civil_x, tbl_nacionalidades_id_c_x, bene_cargo_x, bene_pep_x,
			bene_fechanac_x, tbl_ocupaciones_id_c_x, bene_telefono_x, bene_email_x, 
			-- ini
			bene_telefono_nro_x, bene_telefono_carac_x, bene_domi_calle_x, bene_domi_nro_x, bene_domi_piso_x, bene_domi_dpto_x,
			bene_tipo_soc_x, bene_actividad_x,bene_fescritura_x, bene_snombre_x, bene_ape_x, tbl_paises_id_c_x,
			bene_tdoc_afip_x, bene_nombre2_x, 
			-- fin
			bene_cuit_a, bene_tipo_a, bene_nombre_a, bene_apemat_a, bene_doc_tipo_a, bene_doc_nro_a, 
			bene_domi_a, tbl_localidades_id_c_a, tbl_provincias_id_c_a, 
			bene_sexo_a, bene_estado_civil_a, tbl_nacionalidades_id_c_a, bene_cargo_a, bene_pep_a,
			bene_fechanac_a, tbl_ocupaciones_id_c_a, bene_telefono_a, bene_email_a, 
			-- ini
			bene_telefono_nro_a, bene_telefono_carac_a, bene_domi_calle_a, bene_domi_nro_a, bene_domi_piso_a, bene_domi_dpto_a,
			bene_tipo_soc_a, bene_actividad_a,bene_fescritura_a, bene_snombre_a, bene_ape_a, tbl_paises_id_c_a,
			bene_tdoc_afip_a, bene_nombre2_a
			-- fin
	;
		
	SET msgret = 'OK';
	
	HAY_REGISTROS: WHILE (finished = 0) DO
		
		IF (bene_cuit_a IS NULL) THEN
			
			SET 	bene_tipo_n = bene_tipo_x, 
				bene_nombre_n = bene_nombre_x, 
				bene_apemat_n = bene_apemat_x, 
				bene_doc_tipo_n = bene_doc_tipo_x, 
				bene_doc_nro_n = bene_doc_nro_x, 
				bene_domi_n = bene_domi_x, 
				tbl_localidades_id_c_n = tbl_localidades_id_c_x, 
				tbl_provincias_id_c_n = tbl_provincias_id_c_x, 
				bene_sexo_n = bene_sexo_x, 
				bene_estado_civil_n = bene_estado_civil_x, 
				tbl_nacionalidades_id_c_n = tbl_nacionalidades_id_c_x, 
				bene_cargo_n = bene_cargo_x,
				bene_pep_n = bene_pep_x,
				bene_fechanac_n = bene_fechanac_x,
				tbl_ocupaciones_id_c_n = tbl_ocupaciones_id_c_x,
				bene_telefono_n = bene_telefono_x, 
				bene_email_n = bene_email_x,
				-- ini
				bene_telefono_nro_n = bene_telefono_nro_x, 
				bene_telefono_carac_n = bene_telefono_carac_x, 
				bene_domi_calle_n = bene_domi_calle_x, 
				bene_domi_nro_n = bene_domi_nro_x, 
				bene_domi_piso_n = bene_domi_piso_x, 
				bene_domi_dpto_n = bene_domi_dpto_x,
				bene_tipo_soc_n = bene_tipo_soc_x, 
				bene_actividad_n = bene_actividad_x,
				bene_fescritura_n = bene_fescritura_x, 
				bene_snombre_n = bene_snombre_x, 
				bene_ape_n = bene_ape_x, 
				tbl_paises_id_c_n = tbl_paises_id_c_x,
				bene_tdoc_afip_n = bene_tdoc_afip_x, 
				bene_nombre2_n = bene_nombre2_x,
				-- fin
				alta = TRUE,
				modif = FALSE;
		ELSE
			
			SET 	bene_tipo_n = bene_tipo_a, 
				bene_nombre_n = bene_nombre_a, 
				bene_apemat_n = bene_apemat_a, 
				bene_doc_tipo_n = bene_doc_tipo_a, 
				bene_doc_nro_n = bene_doc_nro_a, 
				bene_domi_n = bene_domi_a, 
				tbl_localidades_id_c_n = tbl_localidades_id_c_a, 
				tbl_provincias_id_c_n = tbl_provincias_id_c_a, 
				bene_sexo_n = bene_sexo_a, 
				bene_estado_civil_n = bene_estado_civil_a, 
				tbl_nacionalidades_id_c_n = tbl_nacionalidades_id_c_a, 
				bene_cargo_n = bene_cargo_a,
				bene_pep_n = bene_pep_a,
				bene_fechanac_n = bene_fechanac_a,
				tbl_ocupaciones_id_c_n = tbl_ocupaciones_id_c_a,
				bene_telefono_n = bene_telefono_a, 
				bene_email_n = bene_email_a,
				-- ini
				bene_telefono_nro_n = bene_telefono_nro_a,
				bene_telefono_carac_n = bene_telefono_carac_a, 
				bene_domi_calle_n = bene_domi_calle_a, 
				bene_domi_nro_n = bene_domi_nro_a, 
				bene_domi_piso_n = bene_domi_piso_a, 
				bene_domi_dpto_n = bene_domi_dpto_a, 
				bene_tipo_soc_n = bene_tipo_soc_a, 
				bene_actividad_n = bene_actividad_a,
				bene_fescritura_n = bene_fescritura_a, 
				bene_snombre_n = bene_snombre_a, 
				bene_ape_n = bene_ape_a, 
				tbl_paises_id_c_n = tbl_paises_id_c_a,
				bene_tdoc_afip_n = bene_tdoc_afip_a, 
				bene_nombre2_n = bene_nombre2_a,
				-- fin
				alta = FALSE,
				modif = FALSE;
		END IF;
		
		
		SET bene_cuit_n = bene_cuit_x; 
                
                MISMO_CUIT: WHILE (finished = 0 AND bene_cuit_x = bene_cuit_n) DO
				
				IF (bene_tipo_x <> '' AND bene_tipo_n <> bene_tipo_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_tipo_n', 'varchar(255)', '', 'reg modif.', bene_tipo_n, bene_tipo_x)
					;
					SET bene_tipo_n = bene_tipo_x, modif = TRUE;
				END IF; 
				
				
				IF (bene_nombre_x <> '' AND bene_nombre_n <> bene_nombre_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_nombre_n', 'varchar(255)', '', 'reg modif.', bene_nombre_n, bene_nombre_x)
					;				
					SET bene_nombre_n = bene_nombre_x, modif = TRUE;
				END IF;
				
				IF (bene_apemat_x <> '' AND bene_apemat_n <> bene_apemat_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_apemat_n', 'varchar(255)', '', 'reg modif.', bene_apemat_n, bene_apemat_x)
					;				
					SET bene_apemat_n = bene_apemat_x, modif = TRUE;
				END IF;
				-- ini
				IF (bene_ape_x <> '' AND bene_ape_n <> bene_ape_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_ape_n', 'varchar(255)', '', 'reg modif.', bene_ape_n, bene_ape_x)
					;				
					SET bene_ape_n = bene_ape_x, modif = TRUE;
				END IF;
				IF (bene_snombre_x <> '' AND bene_snombre_n <> bene_snombre_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_snombre_n', 'varchar(255)', '', 'reg modif.', bene_snombre_n, bene_snombre_x)
					;				
					SET bene_snombre_n = bene_snombre_x, modif = TRUE;
				END IF;				
				IF (bene_nombre2_x <> '' AND bene_nombre2_n <> bene_nombre2_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_nombre2_n', 'varchar(255)', '', 'reg modif.', bene_nombre2_n, bene_nombre2_x)
					;				
					SET bene_nombre2_n = bene_nombre2_x, modif = TRUE;
				END IF;				
				-- fin
				IF (bene_doc_tipo_x <> '0' AND bene_doc_tipo_n <> bene_doc_tipo_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_doc_tipo_n', 'varchar(255)', '', 'reg modif.', bene_doc_tipo_n, bene_doc_tipo_x)
					;				
					SET bene_doc_tipo_n = bene_doc_tipo_x, modif = TRUE;
				END IF;	
				
				IF (bene_doc_nro_x <> 0 AND bene_doc_nro_x <> 999 AND bene_doc_nro_n <> bene_doc_nro_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_doc_nro_n', 'varchar(255)', '', 'reg modif.', bene_doc_nro_n, bene_doc_nro_x)
					;				
					SET bene_doc_nro_n = bene_doc_nro_x, modif = TRUE;
				END IF;	
				
				IF (bene_domi_x <> '' AND bene_domi_n <> bene_domi_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_domi_n', 'varchar(255)', '', 'reg modif.', bene_domi_n, bene_domi_x)
					;				
					SET bene_domi_n = bene_domi_x, modif = TRUE;
				END IF;
				-- ini
				IF (bene_domi_calle_x <> '' AND bene_domi_calle_n <> bene_domi_calle_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_domi_calle_n', 'varchar(255)', '', 'reg modif.', bene_domi_calle_n, bene_domi_calle_x)
					;				
					SET bene_domi_calle_n = bene_domi_calle_x, modif = TRUE;
				END IF;				
				IF (bene_domi_nro_x <> '' AND bene_domi_nro_n <> bene_domi_nro_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_domi_nro_n', 'varchar(255)', '', 'reg modif.', bene_domi_nro_n, bene_domi_nro_x)
					;				
					SET bene_domi_nro_n = bene_domi_nro_x, modif = TRUE;
				END IF;			
				IF (bene_domi_piso_x <> '' AND bene_domi_piso_n <> bene_domi_piso_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_domi_piso_n', 'varchar(255)', '', 'reg modif.', bene_domi_piso_n, bene_domi_piso_x)
					;				
					SET bene_domi_piso_n = bene_domi_piso_x, modif = TRUE;
				END IF;				
				IF (bene_domi_dpto_x <> '' AND bene_domi_dpto_n <> bene_domi_dpto_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_domi_dpto_n', 'varchar(255)', '', 'reg modif.', bene_domi_dpto_n, bene_domi_dpto_x)
					;				
					SET bene_domi_dpto_n = bene_domi_dpto_x, modif = TRUE;
				END IF;	
				-- fin
				
				IF (tbl_localidades_id_c_x <> '0-0' AND tbl_localidades_id_c_n <> tbl_localidades_id_c_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'tbl_localidades_id_c_n', 'varchar(255)', '', 'reg modif.', tbl_localidades_id_c_n, tbl_localidades_id_c_x)
					;				
					SET tbl_localidades_id_c_n = tbl_localidades_id_c_x, modif = TRUE;
				END IF;
				
				IF (TRIM(tbl_provincias_id_c_x) <> '' AND tbl_provincias_id_c_n <> tbl_provincias_id_c_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'tbl_provincias_id_c_n', 'varchar(255)', '', 'reg modif.', tbl_provincias_id_c_n, tbl_provincias_id_c_x)
					;				
					SET tbl_provincias_id_c_n = tbl_provincias_id_c_x, modif = TRUE;
				END IF;
				-- ini
				IF (tbl_paises_id_c_x <> '' AND tbl_paises_id_c_n <> tbl_paises_id_c_x) THEN
					INSERT INTO uif_persona_audit 
						(id, parent_id,date_created,created_by,
						 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
						)
					VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'tbl_paises_id_c_n', 'varchar(255)', '', 'reg modif.', tbl_paises_id_c_n, tbl_paises_id_c_x)
					;				
					SET tbl_paises_id_c_n = tbl_paises_id_c_x, modif = TRUE;
				END IF;				
				-- fin
				-- IF (bene_sexo_x <> '') THEN
					
					IF (bene_sexo_x <> 'S' AND bene_sexo_n <> bene_sexo_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_sexo_n', 'varchar(255)', '', 'reg modif.', bene_sexo_n, bene_sexo_x)
						;					
						SET bene_sexo_n = bene_sexo_x, modif = TRUE;
					END IF;	
					
					IF (bene_estado_civil_x <> '0' AND bene_estado_civil_n <> bene_estado_civil_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_estado_civil_n', 'varchar(255)', '', 'reg modif.', bene_estado_civil_n, bene_estado_civil_x)
						;					
						SET bene_estado_civil_n = bene_estado_civil_x, modif = TRUE;
					END IF;	
					
					IF (bene_pep_x <> 'X' AND bene_pep_n <> bene_pep_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_pep_n', 'varchar(255)', '', 'reg modif.', bene_pep_n, bene_pep_x)
						;					
						SET bene_pep_n = bene_pep_x, modif = TRUE;
					END IF;	
					
					IF (bene_cargo_x <> '' AND bene_cargo_n <> bene_cargo_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_cargo_n', 'varchar(255)', '', 'reg modif.', bene_cargo_n, bene_cargo_x)
						;					
						SET bene_cargo_n = bene_cargo_x, modif = TRUE;
					END IF;	
					
					IF (bene_telefono_x <> '' AND bene_telefono_n <> bene_telefono_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_telefono_n', 'varchar(255)', '', 'reg modif.', bene_telefono_n, bene_telefono_x)
						;					
						SET bene_telefono_n = bene_telefono_x, modif = TRUE;
					END IF;	
					-- ini
					IF (bene_telefono_carac_x <> '' AND bene_telefono_carac_n <> bene_telefono_carac_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_telefono_carac_n', 'varchar(255)', '', 'reg modif.', bene_telefono_carac_n, bene_telefono_carac_x)
						;					
						SET bene_telefono_carac_n = bene_telefono_carac_x, modif = TRUE;
					END IF;	
					IF (bene_telefono_nro_x <> '' AND bene_telefono_nro_n <> bene_telefono_nro_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_telefono_nro_n', 'varchar(255)', '', 'reg modif.', bene_telefono_nro_n, bene_telefono_nro_x)
						;					
						SET bene_telefono_nro_n = bene_telefono_nro_x, modif = TRUE;
					END IF;	
					-- fin
					
					IF (bene_email_x <> '' AND bene_email_n <> bene_email_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_email_n', 'varchar(255)', '', 'reg modif.', bene_email_n, bene_email_x)
						;					
						SET bene_email_n = bene_email_x, modif = TRUE;
					END IF;	
					
					IF (TRIM(tbl_nacionalidades_id_c_x) <> '' AND tbl_nacionalidades_id_c_n <> tbl_nacionalidades_id_c_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'tbl_nacionalidades_id_c_n', 'varchar(255)', '', 'reg modif.', tbl_nacionalidades_id_c_n, tbl_nacionalidades_id_c_x)
						;					
						SET tbl_nacionalidades_id_c_n = tbl_nacionalidades_id_c_x, modif = TRUE;
					END IF;	
					
					IF (tbl_ocupaciones_id_c_x <> '0' AND tbl_ocupaciones_id_c_x <> 'n/i' AND tbl_ocupaciones_id_c_n <> tbl_ocupaciones_id_c_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'tbl_ocupaciones_id_c_n', 'varchar(255)', '', 'reg modif.', tbl_ocupaciones_id_c_n, tbl_ocupaciones_id_c_x)
						;					
						SET tbl_ocupaciones_id_c_n = tbl_ocupaciones_id_c_x, modif = TRUE;
					END IF;	
					
					IF (bene_fechanac_x IS NOT NULL AND bene_fechanac_n <> bene_fechanac_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_fechanac_n', 'varchar(255)', '', 'reg modif.', bene_fechanac_n, bene_fechanac_x)
						;					
						SET bene_fechanac_n = bene_fechanac_x, modif = TRUE;
					END IF;	
					-- ini
					IF (bene_fescritura_x IS NOT NULL AND bene_fescritura_n <> bene_fescritura_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_fescritura_n', 'varchar(255)', '', 'reg modif.', bene_fescritura_n, bene_fescritura_x)
						;					
						SET bene_fescritura_n = bene_fescritura_x, modif = TRUE;
					END IF;					
					IF (bene_actividad_x <> '' AND bene_actividad_n <> bene_actividad_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_actividad_n', 'varchar(255)', '', 'reg modif.', bene_actividad_n, bene_actividad_x)
						;					
						SET bene_actividad_n = bene_actividad_x, modif = TRUE;
					END IF;					
					IF (bene_tipo_soc_x <> '' AND bene_tipo_soc_n <> bene_tipo_soc_x) THEN
						INSERT INTO uif_persona_audit 
							(id, parent_id,date_created,created_by,
							 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
							)
						VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'bene_tipo_soc_n', 'varchar(255)', '', 'reg modif.', bene_tipo_soc_n, bene_tipo_soc_x)
						;					
						SET bene_tipo_soc_n = bene_tipo_soc_x, modif = TRUE;
					END IF;										
					-- fin
				-- END IF;
			
			
				FETCH pers INTO bene_cuit_x, bene_tipo_x, bene_nombre_x, bene_apemat_x, bene_doc_tipo_x, bene_doc_nro_x, 
						bene_domi_x, tbl_localidades_id_c_x, tbl_provincias_id_c_x, 
						bene_sexo_x, bene_estado_civil_x, tbl_nacionalidades_id_c_x, bene_cargo_x, bene_pep_x,
						bene_fechanac_x, tbl_ocupaciones_id_c_x, bene_telefono_x, bene_email_x, 
						-- ini
						bene_telefono_nro_x, bene_telefono_carac_x, bene_domi_calle_x, bene_domi_nro_x, bene_domi_piso_x, bene_domi_dpto_x,
						bene_tipo_soc_x, bene_actividad_x,bene_fescritura_x, bene_snombre_x, bene_ape_x, tbl_paises_id_c_x,
						bene_tdoc_afip_x, bene_nombre2_x, 
						-- fin
						bene_cuit_a, bene_tipo_a, bene_nombre_a, bene_apemat_a, bene_doc_tipo_a, bene_doc_nro_a, 
						bene_domi_a, tbl_localidades_id_c_a, tbl_provincias_id_c_a, 
						bene_sexo_a, bene_estado_civil_a, tbl_nacionalidades_id_c_a, bene_cargo_a, bene_pep_a,
						bene_fechanac_a, tbl_ocupaciones_id_c_a, bene_telefono_a, bene_email_a, 
						-- ini
						bene_telefono_nro_a, bene_telefono_carac_a, bene_domi_calle_a, bene_domi_nro_a, bene_domi_piso_a, bene_domi_dpto_a,
						bene_tipo_soc_a, bene_actividad_a,bene_fescritura_a, bene_snombre_a, bene_ape_a, tbl_paises_id_c_a,
						bene_tdoc_afip_a, bene_nombre2_a
						-- fin
				;
	              
               END WHILE MISMO_CUIT;
               
	       IF (alta) THEN
			SET cnt_altas_ant = cnt_altas;
			SET cnt_altas = cnt_altas + 1;
			
			INSERT INTO uif_persona 
				(id, NAME, date_entered, date_modified, modified_user_id, created_by, description, deleted, 
				assigned_user_id, 
				per_tipo, per_cuit, per_nombre, per_doc_tipo, per_doc_nro, per_domi, 
				per_sexo, per_fechanac, per_estado_civil, per_email, per_telefono, 
				per_pep, per_cargo, per_apemat, tbl_localidades_id_c, tbl_provincias_id_c,
				per_riesgo_actual, per_riesgo_fecultcalc, tbl_nacionalidades_id_c, 
				per_riesgo_prom_act, per_riesgo_prom_max, per_riesgo_feccalcmax, 
				tbl_ocupaciones_id_c,
				-- ini
				per_telefono_numero, per_telefono_carac, per_domi_calle, per_domi_nro, per_domi_piso, per_domi_dpto,
				per_tipo_soc, per_actividad, per_fescritura, per_snombre, per_apellido, tbl_paises_id_c,
				per_tipo_documental_afip, per_nombre2
				-- fin
				)
			VALUES (bene_cuit_n, TRIM(CONCAT(TRIM(CONCAT(TRIM(CONCAT(bene_ape_n,' ',bene_apemat_n)),', ', bene_nombre_n)),' ', bene_snombre_n)), NOW(), NOW(), '1', '1', 
				TRIM(CONCAT(TRIM(CONCAT(TRIM(CONCAT(bene_ape_n,' ',bene_apemat_n)),', ', bene_nombre_n)),' ', bene_snombre_n)), 0, 
				'1', 
				bene_tipo_n, bene_cuit_n, bene_nombre_n, bene_doc_tipo_n, bene_doc_nro_n, bene_domi_n, 
				bene_sexo_n, bene_fechanac_n, bene_estado_civil_n,  bene_email_n, bene_telefono_n, 
				bene_pep_n, bene_cargo_n, bene_apemat_n, tbl_localidades_id_c_n, tbl_provincias_id_c_n, 
				0, NOW(), tbl_nacionalidades_id_c_n, 
				0, 0, NOW(), 
				tbl_ocupaciones_id_c_n,
				-- ini
				bene_telefono_nro_n, bene_telefono_carac_n, bene_domi_calle_n, bene_domi_nro_n, bene_domi_piso_n, bene_domi_dpto_n,
				bene_tipo_soc_n, bene_actividad_n,bene_fescritura_n, bene_snombre_n, bene_ape_n, tbl_paises_id_c_n,
				bene_tdoc_afip_n, bene_nombre2_n
				-- fin
				)
			;
			INSERT INTO uif_persona_audit 
				(id, parent_id,date_created,created_by,
				 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
				)
			VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'name', 'varchar(255)', '', 'reg nuevo', cnt_altas_ant, cnt_altas)
			;			
               END IF;
               
	       IF (modif) THEN
			SET cnt_modif_ant = cnt_modif;
			SET cnt_modif = cnt_modif + 1;
			UPDATE uif_persona p
				SET 	p.name = TRIM(CONCAT(TRIM(CONCAT(TRIM(CONCAT(bene_ape_n,' ',bene_apemat_n)),', ', bene_nombre_n)),' ', bene_snombre_n)),
					p.description = TRIM(CONCAT(TRIM(CONCAT(TRIM(CONCAT(bene_ape_n,' ',bene_apemat_n)),', ', bene_nombre_n)),' ', bene_snombre_n)),
					p.date_modified = NOW(),
					p.per_tipo = bene_tipo_n, 
					p.per_nombre = bene_nombre_n, 
					p.per_doc_tipo = bene_doc_tipo_n, 
					p.per_doc_nro = bene_doc_nro_n, 
					p.per_domi = bene_domi_n, 
					p.per_sexo = bene_sexo_n, 
					p.per_fechanac = bene_fechanac_n,
					p.per_estado_civil = bene_estado_civil_n, 
					p.per_email = bene_email_n,
					p.per_telefono = bene_telefono_n, 
					p.per_pep = bene_pep_n,
					p.per_cargo = bene_cargo_n,
					p.per_apemat = bene_apemat_n, 
					p.tbl_localidades_id_c = tbl_localidades_id_c_n, 
					p.tbl_provincias_id_c = tbl_provincias_id_c_n, 
					p.tbl_nacionalidades_id_c = tbl_nacionalidades_id_c_n, 
					p.tbl_ocupaciones_id_c = tbl_ocupaciones_id_c_n,
					-- ini
					p.per_telefono_numero = bene_telefono_nro_n, 
					p.per_telefono_carac = bene_telefono_carac_n, 
					p.per_domi_calle = bene_domi_calle_n, 
					p.per_domi_nro = bene_domi_nro_n, 
					p.per_domi_piso = bene_domi_piso_n, 
					p.per_domi_dpto = bene_domi_dpto_n,
					p.per_tipo_soc = bene_tipo_soc_n, 
					p.per_actividad = bene_actividad_n,
					p.per_fescritura = bene_fescritura_n, 
					p.per_snombre = bene_snombre_n, 
					p.per_apellido = bene_ape_n, 
					p.tbl_paises_id_c = tbl_paises_id_c_n,
					p.per_tipo_documental_afip = bene_tdoc_afip_n, 
					p.per_nombre2 = bene_nombre2_n
					-- fin
				WHERE p.id = bene_cuit_n AND deleted = 0
				;
				INSERT INTO uif_persona_audit 
					(id, parent_id,date_created,created_by,
					 field_name,data_type,before_value_string,after_value_string,before_value_text,after_value_text
					)
				VALUES (UUID(), bene_cuit_n, NOW(), NOW(), 'name', 'varchar(255)', '', 'reg modif.', cnt_modif_ant, cnt_modif)
				;
		END IF;

	END WHILE HAY_REGISTROS;
	CLOSE pers;
	
	-- por ultimo normalizo por las dudas los name...
	UPDATE uif_persona p
	SET p.name = TRIM(REPLACE(REPLACE(CASE WHEN LEFT(p.name,1) = ',' THEN TRIM(RIGHT(p.name,LENGTH(p.name)-1)) ELSE p.name END,', ,',', '),',,',',')),
	    p.per_apellido = TRIM(REPLACE(REPLACE(CASE WHEN LEFT(p.per_apellido,1) = ',' THEN TRIM(RIGHT(p.per_apellido,LENGTH(p.per_apellido)-1)) ELSE p.per_apellido END,', ,',', '),',,',',')),
	    p.per_apemat = TRIM(REPLACE(REPLACE(CASE WHEN LEFT(p.per_apemat,1) = ',' THEN TRIM(RIGHT(p.per_apemat,LENGTH(p.per_apemat)-1)) ELSE p.per_apemat END,', ,',', '),',,',',')),
	    p.per_nombre = TRIM(REPLACE(REPLACE(CASE WHEN LEFT(p.per_nombre,1) = ',' THEN TRIM(RIGHT(p.per_nombre,LENGTH(p.per_nombre)-1)) ELSE p.per_nombre END,', ,',', '),',,',',')),		    
	    p.per_snombre = TRIM(REPLACE(REPLACE(CASE WHEN LEFT(p.per_snombre,1) = ',' THEN TRIM(RIGHT(p.per_snombre,LENGTH(p.per_snombre)-1)) ELSE p.per_snombre END,', ,',', '),',,',',')),		    
	    p.per_nombre2 = TRIM(REPLACE(REPLACE(CASE WHEN LEFT(p.per_nombre2,1) = ',' THEN TRIM(RIGHT(p.per_nombre2,LENGTH(p.per_nombre2)-1)) ELSE p.per_nombre2 END,', ,',', '),',,',','))
	;
	
	INSERT INTO kk_auditoria VALUES(CONCAT(NOW(),' - PREMIOS_uif_act_personas - fin.'));
	
	-- proceso calculo de matriz de riesgo
	CALL PREMIOS_uif_calculo_matriz('',msgret);
	
	IF (msgret = 'OK') THEN
		INSERT INTO kk_auditoria VALUES(CONCAT(NOW(),' - PREMIOS_uif_act_personas - fin PREMIOS_uif_calculo_matriz.'));
	ELSE
		IF (msgret = 'NO hay período en condiciones de ser procesados.') THEN
			SET msgret = 'OK';
			INSERT INTO kk_auditoria VALUES(CONCAT(NOW(),' - PREMIOS_uif_act_personas - PREMIOS_uif_calculo_matriz. no hay periodos por procesar'));
			INSERT INTO kk_auditoria VALUES(CONCAT(NOW(),' - PREMIOS_uif_act_personas - fin PREMIOS_uif_calculo_matriz.'));
		ELSE
			INSERT INTO kk_auditoria VALUES(CONCAT(NOW(),' - PREMIOS_uif_act_personas - error en ejecucion de PREMIOS_uif_calculo_matriz.',msgret));
			SELECT  'Error en la ejecucion de PREMIOS_uif_calculo_matriz' INTO msgret;			
			LEAVE fin;
		END IF;
	END IF;	
	
	
    END$$

DELIMITER ;