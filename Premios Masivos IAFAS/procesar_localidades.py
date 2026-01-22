import pandas as pd
import numpy as np
import unicodedata

# Configuración de archivos
FILE_IAFAS = 'localidadesER_ordenado.csv'
FILE_CAS = 'TBL_localidades.csv'
OUTPUT_FILE = 'localidadesER_procesado.csv'

def normalizar_texto(texto):
    """
    Normaliza el texto: minúsculas, elimina acentos y espacios extra.
    """
    if pd.isna(texto):
        return ""
    # Convertir a string, minúsculas
    texto = str(texto).lower().strip()
    # Eliminar acentos
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    return texto

def generar_sql(row):
    """
    Genera la sentencia SQL basada en el escenario y los datos de la fila.
    """
    escenario = row['escenario']
    
    # Manejo de valores nulos o vacíos para SQL
    def sql_str(val):
        return f"'{str(val).strip()}'" if pd.notna(val) and str(val).strip() != '' else "NULL"
    
    def sql_int(val):
        return str(int(float(val))) if pd.notna(val) else "0"

    if escenario == '4':
        # INSERT para Escenario 4
        # name <- csv.cp_name + "(" + csv.id + ")"
        name_val = f"{row['cp_name']}({row['id']})"
        desc_val = f"Localidad {row['cp_name']}({row['id']})"
        
        # Mapeo de valores
        campos = {
            'id': sql_str(row['id']),
            'name': f"'{name_val}'",
            'date_entered': 'NOW()',
            'date_modified': 'NOW()',
            'modified_user_id': "'1'",
            'created_by': "'1'",
            'description': f"'{desc_val}'",
            'deleted': '0',
            'assigned_user_id': "'1'",
            'billing_address_city': sql_str(row['cp_name']),
            'loc_cpos': sql_str(row['cp_cpos']),
            'loc_scpo': sql_str(row['cp_scpo']),
            'tbl_provincias_id_c': "'E'",
            'tbl_paises_id_c': "'1'",
            'loc_zona_riesgo': "'N'",
            'tbl_departamentos_id_c': "'E000'",
            'ruta_pac': '0',
            'cant_hab_2010': '0',
            'tbl_nodos_id_c': "'E00'",
            'cant_hab': '0',
            'categoria': "'Poblacion'",
            'ruta_pac_orden': '0',
            'codigo_provincia': "NULL"
        }
        
        columns = ", ".join(campos.keys())
        values = ", ".join(campos.values())
        return f"INSERT INTO tbl_localidades ({columns}) VALUES ({values});"
    
    else:
        # UPDATE para Escenarios 1, 2 y 3
        # Condición: WHERE loc_cpos = csv.cp_cpos AND loc_scpo = csv.cp_scpo
        # Nota: Asumimos loc_scpo en la segunda condición del WHERE basado en la lógica estándar,
        # aunque el prompt tenía un typo repitiendo loc_cpos.
        
        id_cas_limpio = str(row['id_cas']).replace('*', '') # Quitamos el asterisco si existe para el ID
        
        cpos_val = str(row['cp_cpos']).strip()
        scpo_val = str(row['cp_scpo']).strip()
        
        sql = (f"UPDATE tbl_cp_diferentes SET tbl_localidades_id_c = '{id_cas_limpio}' "
               f"WHERE loc_cpos = '{cpos_val}' AND loc_scpo = '{scpo_val}';")
        return sql

def main():
    print("Cargando archivos...")
    # Cargar CSVs con delimitador punto y coma
    try:
        df_iafas = pd.read_csv(FILE_IAFAS, sep=';', dtype=str)
        df_cas = pd.read_csv(FILE_CAS, sep=';', dtype=str)
    except FileNotFoundError as e:
        print(f"Error: No se encontró el archivo. {e}")
        return

    # Normalizar nombres para comparación
    print("Normalizando datos...")
    df_iafas['name_norm'] = df_iafas['name_sin_cp'].apply(normalizar_texto)
    df_cas['name_norm'] = df_cas['name_sin_cp'].apply(normalizar_texto)

    # Inicializar columnas de resultado si no existen o limpiarlas
    df_iafas['escenario'] = ''
    df_iafas['id_cas'] = ''
    df_iafas['sql'] = ''

    print("Procesando escenarios...")
    
    # Iterar sobre cada fila de localidadesER (IAFAS)
    for index, row in df_iafas.iterrows():
        cpos = str(row['cp_cpos']).strip()
        scpo = str(row['cp_scpo']).strip()
        name_iafas = row['name_norm']
        
        match_found = False
        
        # --- ESCENARIO 1: Coincidencia exacta de name, cpos y scpo ---
        match1 = df_cas[
            (df_cas['name_norm'] == name_iafas) & 
            (df_cas['loc_cpos'] == cpos) & 
            (df_cas['loc_scpo'] == scpo)
        ]
        
        if not match1.empty:
            df_iafas.at[index, 'id_cas'] = match1.iloc[0]['id']
            df_iafas.at[index, 'escenario'] = '1'
            continue

        # --- ESCENARIO 2: Coincide name pero falla cpos y/o scpo ---
        match2 = df_cas[df_cas['name_norm'] == name_iafas]
        
        if not match2.empty:
            # Tomamos el primero que coincida en nombre
            df_iafas.at[index, 'id_cas'] = match2.iloc[0]['id']
            df_iafas.at[index, 'escenario'] = '2'
            continue

        # --- ESCENARIO 3: Coincidencia parcial (Abreviaturas) ---
        # Iteramos sobre df_cas para buscar substrings
        for _, cas_row in df_cas.iterrows():
            name_cas = cas_row['name_norm']
            
            # Chequeo bidireccional de contenencia
            if name_cas and name_iafas and ((name_cas in name_iafas) or (name_iafas in name_cas)):
                
                # Chequeo de caso sospechoso "COLON"
                es_sospechoso = (name_cas == 'colon' or name_iafas == 'colon')
                
                if es_sospechoso:
                    # Verificar CP
                    if str(cas_row['loc_cpos']).strip() == cpos:
                        df_iafas.at[index, 'id_cas'] = str(cas_row['id']) + "*"
                        df_iafas.at[index, 'escenario'] = '3'
                        match_found = True
                        break
                    else:
                        # Si es sospechoso (Colon) y el CP no coincide, NO es match, seguimos buscando
                        continue
                else:
                    # No es sospechoso, asignamos match
                    df_iafas.at[index, 'id_cas'] = str(cas_row['id']) + "*"
                    df_iafas.at[index, 'escenario'] = '3'
                    match_found = True
                    break
        
        if match_found:
            continue

        # --- ESCENARIO 4: Ninguno de los anteriores ---
        df_iafas.at[index, 'id_cas'] = '' # Dejar vacío
        df_iafas.at[index, 'escenario'] = '4'

    print("Generando sentencias SQL...")
    df_iafas['sql'] = df_iafas.apply(generar_sql, axis=1)

    # Eliminar columna temporal de normalización antes de guardar
    if 'name_norm' in df_iafas.columns:
        df_iafas = df_iafas.drop(columns=['name_norm'])

    # Guardar archivo procesado
    print(f"Guardando resultados en {OUTPUT_FILE}...")
    df_iafas.to_csv(OUTPUT_FILE, sep=';', index=False, encoding='utf-8-sig')

    # --- Verificación de Duplicados ---
    print("\n--- Verificación de Duplicados en id_cas ---")
    # Filtramos vacíos porque escenario 4 tendrá vacíos y esos no cuentan como duplicados erróneos
    ids_asignados = df_iafas[df_iafas['id_cas'] != '']
    duplicados = ids_asignados[ids_asignados.duplicated(subset=['id_cas'], keep=False)]
    
    if not duplicados.empty:
        print(f"Se encontraron {len(duplicados)} filas con id_cas repetido:")
        # Mostrar columnas relevantes de los duplicados
        print(duplicados[['cp_name', 'cp_cpos', 'escenario', 'id_cas']].sort_values(by='id_cas').to_string())
        
        # Opcional: Guardar reporte de duplicados
        duplicados.to_csv('reporte_duplicados.csv', sep=';', index=False, encoding='utf-8-sig')
        print("Se ha generado un archivo 'reporte_duplicados.csv' con el detalle.")
    else:
        print("No se encontraron valores repetidos en la columna id_cas (excluyendo vacíos).")

    print("\nProceso finalizado con éxito.")

if __name__ == "__main__":
    main()