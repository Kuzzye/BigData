import sys
from bs4 import BeautifulSoup
import boto3
import time
import json
from datetime import date

#configuracion del bucket
S3_BUCKET = 'newsraw1006'
s3 = boto3.client('s3')

#Función para traer el objeto del bucket y convertirlo en csv utilizando BeautifulSoup
def to_csv(file):
    file_content = s3.get_object(Bucket=S3_BUCKET, Key=file)["Body"].read() #obtencion del archivo
    soup = BeautifulSoup(file_content, "html.parser")
    text = "tema|id|tema|link|structure\n"
    for articles in soup.find_all('article'):
        for links in articles.find_all('a'): 
            try:
                link = links["href"]
                tema = link.split('/')[1]
                id_news = links["id"]
                code = soup.select_one(f"a#{id_news}").get_text(strip=True, separator=" ")
                text += f"{code}|{id_news}|{tema}|{link}|{links}\n"
            except:
                ...
    return text

#Periodico : El Tiempo

# Ubicación del .html a traer para convertir
name = "eltiempo"
fecha = date.today()
object_key = f"headlines/raw/periodico={name}/year={fecha.year}/month={fecha.month}/day={fecha.day}/eltiempo-{fecha}.html"

#escribe los datos del archivo transformados
with open('/tmp/data_news.csv','wb+') as f:
    f.write(to_csv(object_key).encode('utf-8'))
    f.close()

#carga el archivo CSV a la ruta "s3://bucket/headlines/final/periodico=eltiempo/year=xxx/month=xxx/day=xxx"
with open("/tmp/data_news.csv", "rb+") as f:
    s3.upload_fileobj(f, "newsraw1006", f"headlines/final/periodico={name}/year={fecha.year}/month={fecha.month}/day={fecha.day}/eltiempo-{fecha}.csv")

#Periodico: El Publimetro

# Ubicación del .html a traer para convertir
name2 = "elpublimetro"
fecha2 = date.today()
object_key2 = f"headlines/raw/periodico={name2}/year={fecha2.year}/month={fecha2.month}/day={fecha2.day}/elpublimetro-{fecha2}.html"

#escribe los datos del archivo transformados
with open('/tmp/data_news2.csv','wb+') as f:
    f.write(to_csv(object_key2).encode('utf-8'))
    f.close()

#carga el archivo CSV a la ruta "s3://bucket/headlines/final/periodico=eltiempo/year=xxx/month=xxx/day=xxx"
with open("/tmp/data_news2.csv", "rb+") as f:
    s3.upload_fileobj(f, "newsraw1006", f"headlines/final/periodico={name2}/year={fecha2.year}/month={fecha2.month}/day={fecha2.day}/elpublimetro-{fecha2}.csv")
