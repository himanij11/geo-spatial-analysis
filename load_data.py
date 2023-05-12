import boto3
from bs4 import BeautifulSoup
from urllib.request import urlopen
from zipfile import *
from io import BytesIO
import http
import pandas as pd
import requests
import zlib
import gzip


session = boto3.session.Session(
        aws_access_key_id='AKIASAMPLE',
        aws_secret_access_key='K++cDguSampleSamplenehmeIIgx6v')
s3Resource = session.client('s3')

url = "http://download.geonames.org/export/dump/"
ext = 'zip'

def upload_obj_to_s3(file_buffer, file_name):
    try:
        s3Resource.put_object(Bucket='geo-spatial-analysis', Body=file_buffer, Key='allCountries/' + file_name)
        print("uploaded file", file_name)
    except Exception as err:
        print(err)


def get_extension(filename):
    return filename.split(".")[1]


def get_filename(filename):
    return filename.split(".")[0]


try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen


def listFD(url, ext=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    urls = []
    for node in soup.find_all('a'):
        node_data = node.get('href')
        node_name = get_filename(node_data)
        if node_data.endswith(ext) and len(node_name) == 2:
            urls.append(url + '/' + node_data)
    return urls


df={}
file_zip_buffer = BytesIO()
file_zip_filtered = ZipFile(file_zip_buffer, mode='w',
              compression=ZIP_DEFLATED)
i=0
for file in listFD(url, ext):
    file_name = file.split('//')[-1]
    print(file_name)
    try:
        file_read = urlopen(file).read()
        file_buffer = BytesIO(file_read)
        file_zip = ZipFile(file_buffer)
        #  to remove readme.txt file from zip
        for name in file_zip.namelist():
            if(get_filename(name) == get_filename(file_name)):
                gzip_object = gzip.compress(file_zip.read(name))
                upload_obj_to_s3(gzip_object, get_filename(name) + ".csv.gz")
    except (http.client.IncompleteRead) as e:
         page = e.partial
    file_zip.close()
    file_buffer.seek(0)
