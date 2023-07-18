import requests
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import smtplib
from email.message import EmailMessage

#AIRFLOW

# argumentos por defecto para el DAG
default_args = {
    'owner': 'DanielMendieta',
    'start_date': datetime(2023,7,11),
    'retries':2,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='Productos_ETL',
    default_args=default_args,
    description='Primer trabajo con Airflow',
    schedule_interval="@daily",
    catchup=False
)

#CONEXIÓN API.

url = "https://random-data-api.com/api/commerce/random_commerce?size=100"
response = requests.get(url)
data = response.json()
tabla = pd.DataFrame(data)

#COMIENZO CON LA LIMPIEZA:

#DESCARTO COLUMNAS POCO RELEVANTE.
del tabla ["id"]
del tabla ["uid"]
del tabla ["price_string"]
del tabla ["promo_code"]

#ME QUEDO SOLO CON LOS PRIMEROS 50 RESULTADOS
tabla.drop(range(50,100), axis = 0, inplace=True)

#BORRAMOS POSIBLES DUPLICADOS
tabla.drop_duplicates()

#ORDENAMOS LOS PRODUCTOS DE VALOR MAS ELEVADO AL MAS ECONOMICO.
tabla.sort_values(by=['price'], inplace=True, ascending=False)

#CAMBIO EL NOMBRE DE LAS COLUMNAS.
tabla.columns= ['Color', 'Sector', 'Material','Nombre', 'Precio']
tabla
    

#PASO 2 - CONEXIÓN A AMAZON REDSHIFT.
def redshiftDB():
    urll="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
    data_base="data-engineer-database"
    user="mendietadaniel1994_coderhouse"
    pwd= 'xxxxxxx'
        
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
            )
        print("Conexión Exitosa.")
               
    except Exception as e: 
        print("Algo salio mal.", str(e))
        
#PASO 3 - CREACIÓN DE LA TABLA.
    try:
        dtypes= tabla.dtypes #NOS MUESTRA QUE TIPO DE DATOS ES EJ: FLOAT, OBJECT ETC
        columnas= list(dtypes.index ) #NOS MUESTRA NOMBRE DE LAS COLUMNAS Y LAS CONVIERTE EN UNA LISTA
        tipos= list(dtypes.values) #NOS MUESTRA EL TIPO DE DATO DE LOS VALORES Y LAS CONVIERTE EN UNA LISTA
        conversorDetipos = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}
        sql_dtypes = [conversorDetipos[str(dtype)] for dtype in tipos] # CONVERTIMOS TODO EN CADENA
        column_defs = [f"{name} {data_type}" for name, data_type in zip(columnas, sql_dtypes)] #DEFINIMOS LAS COLUMNAS Y SU TIPO PARA LA TABLA
        cur = conn.cursor() #APUNTAMOS A LA BASE DE DATOS
        sql = f"CREATE TABLE if not exists Productos (id INT IDENTITY(1,1) PRIMARY KEY, {', '.join (column_defs)});" #CREAMOS TABLA CON CLAVE PRIMARIA
        cur.execute(sql)
        conn.commit()
        print ('Tabla Creada.')
    except Exception as e:
        print("No fue posible crear la tabla.", str(e))  
#PASO 4 - INTRODUCCION DE DATOS Y FINAL DEL PROCESO.           
    try:
        values = [tuple(x) for x in tabla.to_numpy()]
        sqll = f"INSERT INTO Productos ({', '.join(columnas)}) VALUES %s"
        execute_values(cur, sqll, values)
        conn.commit()
        print ("Datos Ingresados.")
    
    except Exception as e:
        print("Error al cargar." , str(e))    
    
    finally:
        conn.close()
        print("Conexión Terminada.")     
        
def enviar_email():
    email_from = "mendietadaniel1994@gmail.com"
    passw = 'XXXXXXXXX'
    email_to = "mendietadaniel1994@gmail.com"
    title = "Datos Cargados"
    
    # Obtener la fecha y hora actual que se envia el mail
    fecha_hora_actual = datetime.now()
    fechaHoraActual = fecha_hora_actual.strftime("%d/%m/%Y %H:%M")

  
    body = """Etl realizado con exito, los datos ya estan cargados 
    en una tabla de Amazon Redshift la misma se puede visualizar asi:\n {} ,\n {}""".format(tabla, fechaHoraActual)

        
    email = EmailMessage()
    email['From'] = email_from
    email['To'] = email_to
    email['Subject'] = title
    email.set_content(body)
        
    try:
        # Establecer la conexión segura con el servidor SMTP
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(email_from, passw)
                smtp.send_message(email)
            print("¡Correo electrónico enviado!")
            
    except Exception as e:
        print("Error al enviar el correo electrónico:", str(e))

        
           
#TASKS AIRFLOW        
task_1 = PythonOperator(
    task_id='Conexion_Creacion_Insercion',
    python_callable=redshiftDB,
    dag=BC_dag,
)

task_2 = PythonOperator(
        task_id='Enviar_correo',
        python_callable=enviar_email,
        dag=BC_dag,
    )
       
task_1 >> task_2
