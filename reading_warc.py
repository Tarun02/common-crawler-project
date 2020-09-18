from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from html.parser import HTMLParser

import requests
import gzip

""" 
Required Fields:

URI, Content-length, IP Address, server, All the meta-content
"""

html_parser = HTMLParser()

with gzip.open('warc.paths.gz') as file_data:
    
    for line in file_data:
        
        http_url = 'https://commoncrawl.s3.amazonaws.com/' + line.decode("utf-8").strip()
        resp = requests.get(http_url, stream=True)

        for nested_record in ArchiveIterator(resp.raw, arc2warc=True):
            if nested_record.rec_type == 'response':

                web_uri = nested_record.rec_headers.get_header('WARC-Target-URI')
                content_length = nested_record.rec_headers.get_header('Content-length')
                server_ip_address = nested_record.rec_headers.get_header('WARC-IP-Address')
                server_name = nested_record.http_headers['Server']
                
                web_page = BeautifulSoup(nested_record.content_stream().read(), "html.parser")
                web_page_title = web_page.title.string

                print(web_page.meta)
                metadata_dict = {}
                for tag in web_page_metadata:
                    print(tag.get("property",None))
                    metadata_dict[tag.get("property",None)] = tag.get("content",None)

                print(metadata_dict)
                break
        break