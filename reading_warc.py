from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from html.parser import HTMLParser

import requests
import gzip
import boto3

""" 
Required Fields:

URI, Content-length, IP Address, server, Title, All the meta-content
"""

html_parser = HTMLParser()

bucketname = 'yernt-bgdata'
filename = 'common-crawl/input_data/warc.paths.gz'

s3 = boto3.resource('s3')
s3.Bucket(bucketname).download_file(filename, 'warc.paths.gz')

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
                
                web_page = BeautifulSoup(nested_record.content_stream().read(), "html.parser", from_encoding='iso-8859-8')
                if web_page.title:
                    title = web_page.title.string
                
                final_dict = {}
                metadata_dict = {}
                try:
                    for tag in web_page.find_all('meta'):
                        if tag:
                            temp_dict = tag.attrs
                            for key in (temp_dict.keys() | metadata_dict.keys()):
                                if key in temp_dict: final_dict.setdefault(key, []).append(temp_dict[key])
                                if key in metadata_dict: final_dict.setdefault(key, []).append(metadata_dict[key])
                        temp_dict = final_dict
                except:
                    print(metadata_dict)
            final_data.extend([web_uri, content_length, server_ip_address, server_name, title, final_dict])
    
    final_df = spark.createDataFrame(final_data, user_schema)

final_df.write.json('s3://yernt-bgdata/common-crawl/output_data/')
""" 
Now need to make this into a Spark Code and push the final values into JSON format.
"""