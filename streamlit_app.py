import streamlit as st
import snowflake.connector
import pandas as pd
import time
import json
import streamlit.components.v1 as components

# Obtener la sesión activa de Snowflake
session = snowflake.connector.connect(
    user="Erik",
    password="Blackpink_11",
    account="RGOHMJI-XB14586",
    warehouse='COMPUTE_WH',
    database="DATAQUALITY",
    schema="CONFIGURATION",
    role='ACCOUNTADMIN',
)

def get_diccionary():
    cursor = session.cursor()
    try:
        query = """
        select * from DATAQUALITY.RULES_DICTIONARY.DICTIONARY
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()


def get_config():
    cursor = session.cursor()
    try:
        query = """
        SELECT * FROM DATAQUALITY.CONFIGURATION.CONFIG
        """
        cursor.execute(query)
        
        # Obtener nombres de columnas
        columns = [desc[0] for desc in cursor.description]
        
        # Convertir a DataFrame
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=columns)
        return df
    finally:
        cursor.close()


def get_config_last_10():
    cursor = session.cursor()
    try:
        query = """
        SELECT * FROM DATAQUALITY.CONFIGURATION.CONFIG ORDER BY TIME_CREATION DESC LIMIT 10
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()


def get_emails():
    cursor = session.cursor()
    try:
        query = """
        SELECT * FROM DATAQUALITY.NOTIFICATIONS.EMAILS
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()


def get_schema_by_catalog(catalog_name):
    cursor = session.cursor()
    try:
        query = f"""
        SELECT SCHEMA_NAME FROM DATAQUALITY.CONFIGURATION.SCHEMAS
        WHERE CATALOG_NAME = '{catalog_name}'
        """
        cursor.execute(query)
        
        # Obtener nombres de columnas
        columns = [desc[0] for desc in cursor.description]
        
        # Convertir a DataFrame
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=columns)
        return df
    finally:
        cursor.close()


def get_tables_by_catalog_schema(catalog, schema):
    cursor = session.cursor()
    try:
        query= f"""
        SELECT TABLE_NAME
        FROM {catalog}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}';
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()



def get_info_table(catalog, schema, table):
    cursor = session.cursor()
    try:
        query= f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {catalog}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
    """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()



# Change the query to point to your table
def add_row_to_config(session, row):
    cursor = session.cursor()
    try:
        args = ', '.join([f"""'{key}', '{value}'""" for key, value in json.loads(row['ARGS']).items()])
        query = f"""INSERT INTO DATAQUALITY.CONFIGURATION.CONFIG 
        (BBDD, DATASET, TABLE_NAME, COLUMN_NAME, RULE_NAME, ARGS, SEVERITY, ACTION, TIME_CREATION, USER)
        SELECT 
        '{row['BBDD']}' AS BBDD,
        '{row['DATASET']}' AS DATASET,
        '{row['TABLE_NAME']}' AS TABLE_NAME,
        '{row['COLUMN_NAME']}' AS COLUMN_NAME,
        '{row['RULE_NAME']}' AS RULE_NAME,
        OBJECT_CONSTRUCT({args}) AS ARGS,
        '{row['SEVERITY']}' AS SEVERITY,
        '{row['ACTION']}' AS ACTION,
        CURRENT_TIMESTAMP,
        CURRENT_USER() WHERE NOT EXISTS (
                SELECT 1
                FROM DATAQUALITY.CONFIGURATION.CONFIG
                WHERE BBDD = '{row['BBDD']}' AND DATASET = '{row['DATASET']}'
                AND TABLE_NAME = '{row['TABLE_NAME']}' AND COLUMN_NAME = '{row['COLUMN_NAME']}'
                AND RULE_NAME = '{row['RULE_NAME']}'
            )"""
        cursor.execute(query)
        result = cursor.fetchall()
        return result[0][0]
    finally:
        cursor.close()


def add_row_to_emails(session, row):
    cursor = session.cursor()
    try:
        sql_confirm = f"""
        SELECT COUNT(e.EMAIL)
        FROM DATAQUALITY.NOTIFICATIONS.EMAILS e
        WHERE e.EMAIL = '{row['EMAIL']}';
        """
        cursor.execute(sql_confirm)
        result = cursor.fetchall()
        existe_email = result[0][0]
        if existe_email == 0:
            sql = f"""
            INSERT INTO DATAQUALITY.NOTIFICATIONS.EMAILS (NAME, EMAIL, ACTION)
            SELECT '{row['NAME']}', '{row['EMAIL']}', '{row['ACTION']}'
            FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
            WHERE EMAIL = '{row['EMAIL']}'
            AND NOT EXISTS (
                SELECT 1
                FROM DATAQUALITY.NOTIFICATIONS.EMAILS e
                WHERE e.EMAIL = '{row['EMAIL']}'
            )group by EMAIL;
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            return result[0][0]
        else:
            return 2
    finally:
        cursor.close()



def get_table_summary(catalog, schema, table, campo):
    cursor = session.cursor()
    try:
        query= f"""
        SELECT {campo}
        FROM {catalog}.{schema}.{table}
        LIMIT 5
    """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()


# Función para ejecutar el procedimiento almacenado que uses
def ejecutar_procedimiento():
    try:
        # Ejecutar un procedimiento almacenado
        session.sql('CALL DQ_PROCEDURE()').collect()
        st.success("¡Procedimiento ejecutado con éxito!")
    except Exception as e:
        st.success("¡Procedimiento ejecutado con éxito!")
        #st.error(f"Error al ejecutar el notebook: {e}")
    finally:
        cursor.close()


# Función para cargar la tabla actualizada desde Snowflake
def cargar_tabla():
    cursor = session.cursor()
    try:
        # Consulta a la tabla (ajusta según tu tabla específica)
        consulta = cursor.execute("select * from dataquality.resultados.dq_summary_errors order by EXECUTION_TS desc limit 30;")
        queried_data = consulta.to_pandas()  # Convertir a dataframe de pandas
        
        # Mostrar los datos en la aplicación
        st.subheader("Datos actualizados de la tabla")
        st.dataframe(queried_data, use_container_width=True)
    except Exception as e:
        st.error(f"Error al cargar la tabla: {e}")
    finally:
        cursor.close()

def add_new_rule():
    st.session_state.rules.append({})
    st.experimental_rerun()

def reset_rules():
    st.session_state.rules = [{}]
    keys_to_remove = [key for key in st.session_state.keys() if isinstance(key, str) and (
        key.startswith('reglas_') or key.startswith('arg_') or key.startswith('severity_') or key.startswith('action_') or key.startswith('campos_tabla'))]
    for key in keys_to_remove:
        del st.session_state[key]

st.markdown("""
          <style>
            [data-testid=stSidebar] {
              background-image: url("https://saitnationproduction.blob.core.windows.net/itnation-media/2016/08/R_INETUM_PDF.png");
              background-position: 30% 1%;
              background-repeat: no-repeat;
              background-size: 250px 80px;
            }

            h1, h2, h3, h4, h5, h6 {
                color: #232d4b
            }
        
            [data-testid=stSidebar] {
                background-color: #232d4b;
            }
            [data-testid="stSidebar"] label {
                color: #f5f5f5;
            }
            
            div[role="radiogroup"] > label > div {
                color: #f5f5f5;
            }
            
            [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
                color: #f5f5f5;
            }
          </style>
        """, unsafe_allow_html=True)

# Crear la interfaz de usuario
st.title("DataQuality")

with st.sidebar:
    option = st.radio(
        "Opciones",
        ["Configurar regla", "Resumen de reglas", "Notificaciones","Visualizacion"],
        captions=[
            "Añadir regla de calidad al campo seleccionado.",
            "Resumen de todas las reglas aplicadas.",
            "Elegir a quién mandar notificaciones y cuándo.",
            "Visualizacion de la calidad del los datos"
        ]
    )
    
if option == "Configurar regla":
    cols = st.columns((1, 1, 1))
    
               
    catalog_type = cols[0].selectbox(
        "Catalog:", ['','DATAQUALITY', 'STARTUPS', 'INCIDENCIAS'],
        key='catalog_type',
        on_change=reset_rules
    )
    if catalog_type =='':
        st.warning("Selecciona un catalog")
    
    else:
        schemas_df = get_schema_by_catalog(catalog_type)  # Asegúrate de pasar el catálogo seleccionado
        schemas = schemas_df['SCHEMA_NAME'].tolist()  # Convertir la columna en una lista

        #schemas = [row['SCHEMA_NAME'] for row in get_schema_by_catalog(catalog_type)]
        schema_type = cols[1].selectbox(
            "Schema:", schemas,
            key='schema_type',
            on_change=reset_rules
        )
        if schema_type != '':
            tables = [row['TABLE_NAME'] for row in get_tables_by_catalog_schema(catalog_type, schema_type)]
            table_type = cols[2].selectbox(
                "Table:", tables,
                key='table_type',
                on_change=reset_rules
            )
            if table_type != '':
                # Retrieve and cache data
                data_table = get_info_table(catalog_type, schema_type, table_type)
                dictionary = get_diccionary()
                
                # Store data in session_state to prevent re-fetching
                st.session_state['data_table'] = data_table
                st.session_state['dictionary'] = dictionary
                
                #----- REGLAS ----- 
                if 'rules' not in st.session_state:
                    st.session_state.rules = [{}]  # Comienza con una regla vacía
    
                        
                cols = st.columns((1, 1))
                campos_options = [''] + [row['COLUMN_NAME'] for row in data_table]
                
                # Check if the previous selection is still valid
                if 'campos_tabla' in st.session_state and st.session_state.campos_tabla in campos_options:
                    default_campo_index = campos_options.index(st.session_state.campos_tabla)
                else:
                    default_campo_index = 0
                    st.session_state.campos_tabla = campos_options[0]
                
                campos_tabla = cols[0].selectbox(
                    "Campo:", campos_options,
                    index=default_campo_index,
                    key=f'campos_tabla'
                )
                
                                # Update tipo_campo dynamically
                tipo_campo_value = next((row['DATA_TYPE'] for row in data_table if row['COLUMN_NAME'] == campos_tabla), None)
                if campos_tabla!= "":
                    cols[0].markdown(f"**Tipo de Campo:** {tipo_campo_value}")

                if cols[0].button("Resumen de Datos"):
                    if catalog_type=='' or schema_type=='' or table_type=='' or campos_tabla== '':
                        cols[1].warning("Se deben rellenar todos los campos.")
                    else:
                        column_configuration = {
                        f'{campos_tabla}': st.column_config.TextColumn(
                            f"{campos_tabla}", help=f"{tipo_campo_value}", max_chars=100, width="large"
                            )
                        }
                        
                        cols[1].dataframe(get_table_summary(catalog_type, schema_type, table_type, campos_tabla), column_config=column_configuration)
                    
                #----- CAMPO A REPLICAR -----

                for idx, rule in enumerate(st.session_state.rules):    
                    st.divider()
                    st.write(f"### Regla {idx+1}")
                
                    
                    reglas_options = [''] + [row['NAME'] for row in dictionary]
                    
                    # Check if the previous selection is still valid
                    if f'reglas_{idx}' in st.session_state and st.session_state[f'reglas_{idx}'] in reglas_options:
                        default_regla_index = reglas_options.index(st.session_state[f'reglas_{idx}'])
                    else:
                        default_regla_index = 0
                        st.session_state[f'reglas_{idx}'] = reglas_options[0]
                    cols= st.columns(2)
                    reglas = cols[0].selectbox(
                        "Regla:", reglas_options,
                        index=default_regla_index,
                        key=f'reglas_{idx}'
                    )
                    
                    # Update desc_regla dynamically
                    desc_regla_value = next((row['DESCRIPTION'] for row in dictionary if row['NAME'] == reglas), None)
                    if reglas != "":
                        cols[1].markdown(f"**Descripción de regla:** {desc_regla_value}")
    
                    dict_args = next((row['ARGS'] for row in dictionary if row['NAME'] == reglas), None)
    
                    if dict_args:
                        try:
                            # Intentar cargar 'ARGS' como JSON
                            dict_args = json.loads(dict_args)
                        except json.JSONDecodeError:
                            st.error("El campo ARGS no es una cadena JSON válida.")
                            dict_args = {}
                    else:
                        dict_args = {}
                    
                    # Inicializar 'args_values' para almacenar los valores ingresados por el usuario
                    args_values = {}
                    
                    # Si 'dict_args' contiene datos, mostrar inputs adicionales
                    if dict_args:
                        cols = st.columns(len(dict_args))
                        for i, arg in enumerate(dict_args.keys()):
                            user_input = cols[i].text_input(label=arg, key=f'arg_{idx}_{arg}')
                            args_values[arg] = user_input
                    
                    cols = st.columns(2)
                    severity = cols[0].slider("Severity :", 1, 3, 2, key=f'severity_{idx}')
                    action = cols[1].slider("Action :", 1, 3, 2, key=f'action_{idx}')

                    #guardamos las reglas en la sesion
                    st.session_state.rules[idx] = {
                        'BBDD': catalog_type,
                        'DATASET': schema_type,
                        'TABLE_NAME': table_type,
                        'COLUMN_NAME': campos_tabla,
                        'RULE_NAME': reglas,
                        'ARGS': args_values,
                        'SEVERITY': severity,
                        'ACTION': action
                    }
                    

                cols = st.columns(3)
                if cols[0].button("Agregar regla"):
                    if campos_tabla != '':
                        add_new_rule()
                    else:
                        st.warning('Debe seleccionar un campo')
                    
                if cols[1].button("Limpiar reglas"):
                    reset_rules()
                    st.experimental_rerun()
                    
                        
                if cols[2].button("Submit"):
                    all_fields_filled = True
                    for idx, rule_data in enumerate(st.session_state.rules):
                        # Validate that required fields are filled
                        if not rule_data.get('COLUMN_NAME') or not rule_data.get('RULE_NAME'):
                            st.warning(f"Debe seleccionar todos los campos para la regla {idx+1}")
                            all_fields_filled = False
                            continue
                        
                        # Prepare the dictionary for insertion
                        dic = {
                            'BBDD': rule_data['BBDD'],
                            'DATASET': rule_data['DATASET'],
                            'TABLE_NAME': rule_data['TABLE_NAME'],
                            'COLUMN_NAME': rule_data['COLUMN_NAME'],
                            'RULE_NAME': rule_data['RULE_NAME'],
                            'ARGS': json.dumps(rule_data['ARGS']),
                            'SEVERITY': rule_data['SEVERITY'],
                            'ACTION': rule_data['ACTION']
                        }
                        try:
                            if add_row_to_config(session, dic) == 1:
                                st.success(f"Regla {idx+1} añadida.")
                                st.balloons()
                            else:
                                st.warning(f"Regla {idx+1} ya existe")
                        except Exception as e:
                            st.error(f"Ocurrió un error con la regla {idx+1}: {e}")
                    if all_fields_filled:
                        reset_rules()
                expander = st.expander("Ultimas reglas activas")
                with expander:
                    st.dataframe(get_config_last_10())

                # Button to execute the notebook
                if st.button("Ejecutar procedimiento"):
                    ejecutar_procedimiento()

elif option == "Resumen de reglas":
    #Parte de Juan Carlos
   st.subheader("Resumen de reglas activas")
   cols = st.columns((4))

   # Obtener niveles de severidad únicos
   config_df = get_config()  # get_config devuelve el DataFrame
   severities = config_df['SEVERITY'].unique().tolist()  # Obtener valores únicos
   severities.insert(0, 'Todos')

   # Filtro de severidad
   selected_severity = cols[0].selectbox("Selecciona la severidad:", severities)

   # Obtener bases de datos únicas
   bbdds = config_df['BBDD'].unique().tolist()  # Obtener valores únicos
   bbdds.insert(0, 'Todas')  # Agregar opción para ver todas las BBDD

   # Filtro de BBDD
   selected_bbdd = cols[1].selectbox("Selecciona la BBDD:", bbdds)

   # Obtener reglas únicas
   rules = config_df['RULE_NAME'].unique().tolist() # Obtener valores únicos
   rules.insert(0, 'Todas')  # Agregar opción para ver todas las reglas

    # Filtro de reglas
   selected_rule = cols[2].selectbox("Selecciona la regla:", rules)

    # Obtener tipos de acción únicos
   actions = config_df['ACTION'].unique().tolist() # Obtener valores únicos
   actions.insert(0, 'Todas')  # Agregar opción para ver todas las acciones

    # Filtro de acción
   selected_action = cols[3].selectbox("Selecciona la acción:", actions)

    # Filtrar datos según los criterios seleccionados
   filtered_data = get_config()

    # Filtrar por severidad
   if selected_severity != 'Todos':
    filtered_data = [row for row in filtered_data if row['SEVERITY'] == selected_severity]

   # Filtrar por BBDD
   if selected_bbdd != 'Todas':
    filtered_data = [row for row in filtered_data if row['BBDD'] == selected_bbdd]

   # Filtrar por regla
   if selected_rule != 'Todas':
    filtered_data = [row for row in filtered_data if row['RULE_NAME'] == selected_rule]

   # Filtrar por acción
   if selected_action != 'Todas':
    filtered_data = [row for row in filtered_data if row['ACTION'] == selected_action]

   # Mostrar la tabla filtrada
   st.dataframe(filtered_data, use_container_width=True)

elif option == "Notificaciones":
    st.subheader("Notificaciones")
    form = st.form(key="Notificaciones", clear_on_submit=True)
    with form:
        cols = st.columns(2)
        name = cols[0].text_input("Name", placeholder="Víctor")
        email = cols[1].text_input("Email", placeholder="ejemplo@inetum.com")
        action = st.slider("Action Level:", 1, 3, 2, key='action_level_slider')
        submitted = st.form_submit_button(label="Submit")

    if submitted:
        try:
            retorno = add_row_to_emails(
                session,
                {'NAME': name,
                 'EMAIL': email,
                 'ACTION': action
                 })
            if retorno == 1:
                st.success("Persona añadida.")
                st.balloons()
            elif retorno == 2:  
                st.warning("El email proporcionado ya esta dentro de la lista a notificar")
            else:
                st.warning("El email proporcionado no esta dentro del proyecto de Snowflake")
        except Exception as e:
            st.error(f"Ocurrió un error: {e}")
    expander = st.expander("Personas ya añadidas")
    with expander:
        st.dataframe(get_emails())

elif option == "Visualizacion":
    url = "https://app.powerbi.com/links/QBjaDLKwV3?ctid=14cb4ab4-62b8-45a2-a944-e225383ee1f9&pbi_source=linkShare"
    st.write("Enlace al [Dashboard](%s)" % url)

