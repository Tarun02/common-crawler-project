from warcio.archiveiterator import ArchiveIterator
from zipfile import ZipFile
import ujson as json
import requests
import gzip

""" 
Required Fields:

URI, Content-length, IP Address, server, All the meta-content
"""

with gzip.open('warc.paths.gz') as file_data:
    
    for line in file_data:
        http_url = 'https://commoncrawl.s3.amazonaws.com/' + line.strip()
        resp = requests.get(http_url, stream=True)

        for nested_record in ArchiveIterator(resp.raw, arc2warc=True):
            if nested_record.rec_type == 'response':
                #print(nested_record.rec_headers.get_header('Content-length') + nested_record.rec_headers.get_header('WARC-Target-URI'))
                print(nested_record.rec_headers)
                print(nested_record.http_headers)
                print(nested_record.content_type)
                print(nested_record.content_stream().read())