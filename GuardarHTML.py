import sys
import requests
from datetime import date
import boto3

S3_BUCKET = 'newsraw1006'
s3 = boto3.client('s3')

# Función de carga de archivos en el bucket
def upload_file(file_name, bucket, object_name=None):
  if object_name is None:
    object_name = os.path.basename(file_name)
    try:
      response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e: 
      logging.error(e)
      return False
    return True   

# Periodico: El Tiempo
url = "https://www.eltiempo.com"
name = "eltiempo"
fecha = date.today()
response = requests.get(url)
s3 = boto3.client('s3')

#Escribe el archivo creado
with open("/tmp/eltiempo.html",'wb+') as f: 
    f.write(response.content)
    f.close()
#Lee el archivo creado para almacenarlo en el Bucket "s3://bucket/headlines/raw/periodico=eltiempo/year=xxx/month=xxx/day=xxx"
with open("/tmp/eltiempo.html", "rb+") as f:
    s3.upload_fileobj(f, S3_BUCKET, f"headlines/raw/periodico={name}/year={fecha.year}/month={fecha.month}/day={fecha.day}/eltiempo-{fecha}.html")
    f.close()

# Periodico El Publimetro
url2 = "https://www.publimetro.co//"
name2 = "elpublimetro"
fecha2 = date.today()
response2 = requests.get(url2)
s3 = boto3.client('s3')

#Escribe el archivo creado
with open("/tmp/elpublimetro.html",'wb+') as f: 
    f.write(response2.content)
    f.close()
#Lee el archivo creado para almacenarlo en el Bucket "s3://bucket/headlines/raw/periodico=elpublimetro/year=xxx/month=xxx/day=xxx"
with open("/tmp/elpublimetro.html", "rb+") as f:
    s3.upload_fileobj(f, S3_BUCKET, f"headlines/raw/periodico={name2}/year={fecha2.year}/month={fecha2.month}/day={fecha2.day}/elpublimetro-{fecha2}.html")
    f.close()
