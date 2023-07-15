from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from docker.types import Mount
from datetime import datetime, timedelta,date
from sqlalchemy import create_engine
import pandas as pd
import requests
import json

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def fetch_web_data(ti):    

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 
    url_bmkg = dag_var_bmkg["link_bmkg"]

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    df = pd.DataFrame(columns = ['Tanggal','Jam','DateTime','Coordinates','Lintang','Bujur','Magnitude','Kedalaman','Wilayah','Potensi'])

    page = requests.get(url_bmkg).text
    data = json.loads(page)

    for i in data['Infogempa']['gempa']:
        df = df.append(i,ignore_index='TRUE')    

    #Rerformatting schema data.
    df['DateTime']= pd.to_datetime(df['DateTime'])    
    df['Tanggal']= pd.to_datetime(df['DateTime']).dt.date
    df['Jam WIB']= df['Jam'].str.replace(" WIB","")
    df['Jam WIB']= pd.to_datetime(df['Jam WIB']).dt.time
    df['Jam']= pd.to_datetime(df['DateTime']).dt.time
    df['Magnitude']= pd.to_numeric(df['Magnitude'])

    df.rename(columns={'DateTime':'date_time', 'Jam WIB':'jam_wib'},inplace='TRUE')

    df.columns= df.columns.str.lower()
    
    #Create temp_data for current/today gempa reading before write to main table, avoid duplication
    df.to_sql(name='temp_data',con=engine,index=False, if_exists='replace')

def check_earthquake(ti):    

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 
    url_bmkg = dag_var_bmkg["link_bmkg"]

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    df = pd.read_sql_query("select count(*) as check from gempa where tanggal = current_date and send_email is null", con=engine)

    if df['check'].iloc[0] >= 1:
        return 'send_notification_task'
    else:
        return 'no_earthquake_task'

def send_notification(ti):
    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 
    url_bmkg = dag_var_bmkg["link_bmkg"]

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    query = "SELECT * FROM gempa where tanggal = current_date and send_email is null"
    rows = engine.execute(query).fetchall()

    for row in rows:
        date_time = row['date_time']
        coordinate = row['coordinates']
        subject = 'Your Subject'
        body = 'Your Email Body'

        mail_content = ("Perhatian, telah terjadi gempa dengan data sebagai berikut: \n Tanggal : " + str(row['tanggal']) +"\n Jam :" + str(row['jam_wib']).replace("0 days","") + " WIB" + "\n Coordinate : " + str(row['coordinates']) + "\n Magnitude : " + str(row['magnitude'])) + "\n Kedalaman : " + str(row['kedalaman']) + "\n Wilayah : " + str(row['wilayah']) + "\n Potensi : " + str(row['potensi']) + "\n\n Sumber data : https://data.bmkg.go.id "
        
        send_mail(mail_content)
        # Update column after mail sent
        update_mail_status = engine.execute("update gempa "
                                            "set send_email =  CURRENT_TIMESTAMP "
                                            "where date_time ='" + str(row['date_time']) + "' and  "
                                            "coordinates = '" + str(row['coordinates']) + "'"
                                      ) 

def send_mail(mail_content):
    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    sender_address = dag_var_bmkg["sender_address"] 
    sender_pass = dag_var_bmkg["sender_pass"] 
    receiver_address = dag_var_bmkg["receiver_address"] 

    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Peringatan Gempa ' + str(date.today())  #The subject line
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

def no_earthquake(ti):
    print('No earthquake today')


with DAG("DAG_bmkg", start_date=datetime(2023, 7, 15),
    schedule_interval="*/5 * * * *", catchup=False) as dag:

    create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='postgres_airflow',
        sql="create table if not exists gempa( "
                                            "tanggal date, "
                                            "jam time, "
                                            "date_time timestamptz, "
                                            "coordinates varchar(255), "
                                            "lintang varchar(255), "
                                            "bujur varchar(255), "
                                            "magnitude float, "
                                            "kedalaman varchar(255), "
                                            "wilayah varchar(255), "
                                            "potensi varchar(255), "
                                            "jam_wib time, "
                                            "send_email timestamp);"
    )
    
    move_table_task = PostgresOperator(
        task_id='move_table_task',
        postgres_conn_id='postgres_airflow',
        sql="insert into gempa "
                                        "(tanggal,jam,date_time,coordinates,lintang,bujur,magnitude,kedalaman,wilayah,potensi,jam_wib) "
                                        "select tanggal,jam,date_time,coordinates,lintang,bujur,magnitude,kedalaman,wilayah,potensi,jam_wib "
                                        "from temp_data where CONCAT(temp_data.date_time,temp_data.Coordinates) "
                                        "not in (select concat(gempa.date_time,gempa.coordinates) "
                                        "from gempa);"
    )

    fetch_web_data_task = PythonOperator(
        task_id="fetch_web_data_task",
        python_callable= fetch_web_data
    )      
    
    check_earthquake_task = BranchPythonOperator(
        task_id='check_earthquake_task',
        python_callable=check_earthquake
    )

    send_notification_task = PythonOperator(
        task_id='send_notification_task',
        python_callable=send_notification
    )

    no_earthquake_task = PythonOperator(
        task_id='no_earthquake_task',
        python_callable=no_earthquake
    )

    create_table_task >> fetch_web_data_task >> move_table_task >> check_earthquake_task >> send_notification_task
    create_table_task >> fetch_web_data_task >> move_table_task >> check_earthquake_task >> no_earthquake_task
