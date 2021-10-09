# Author: Arut Selvan Dhanasekaran

from lxml import html
from urllib.parse import urljoin
from image_scraper import ImgScrapy
from langdetect import detect
import wget
import warc
import uuid
import os
import csv
from constants import context_text_min_length,languages

class Utils:
    def __init__(self):
        pass
    
    def get_warc_file_paths(self,segments,paths_file):
        warc_file_paths = {}
        with open(paths_file) as f:
            for line in f:
                for segment in segments:
                    if segment in line:
                        if segment not in warc_file_paths:
                            warc_file_paths[segment]=[line.replace('\n','')]
                        else:
                            warc_file_paths[segment].append(line.replace('\n',''))
        return warc_file_paths

    def read_doc(self,record):
        url = record.url
        html_tree = None

        if url:
            payload = record.payload.read()
            header, html_string = payload.split(b'\r\n\r\n', maxsplit=1)
            
            if len(html_string)>0:
                try:
                    html_tree = html.fromstring(html_string)
                except:
                    html_tree = None
        return url, html_tree

    def acquire_links(self, dom, page_url, warc_file, segment_name):
        """
        Method to get images' download links
        """
        # print(dom)
        images = []
        img_paths = dom.xpath('//img')
        for img in img_paths:
            try:
                img_parent = img.getparent()
                prev_node = img.getprevious()
                next_node = img.getnext()
                context_text = ''
                alt_text = ''
                lang = None
                if img_parent.tag in ['p']:
                    context_text += img_parent.text_content() or ''
                if context_text=='':
                    if prev_node.tag in ['p']:
                        context_text+= '\n' + prev_node.text_content() or ''
                    if next_node.tag in ['p']:
                        context_text+= '\n' + next_node.text_content() or ''
                    alt_text =  img.xpath('./@alt') or ''
                    lang = detect(context_text)
            except:
                continue
            finally:
                try:
                    if len(img.xpath('./@src'))>0 and len(context_text)>context_text_min_length and lang in languages:
                        images.append({
                            "uuid": str(uuid.uuid4()),
                            "img_file": img.xpath('./@src')[0].split('/')[-1],
                            "img_local_path": img.xpath('./@src')[0],
                            "img_link": urljoin(page_url,img.xpath('./@src')[0]),
                            "alt_text": alt_text[0] if len(alt_text)>0 else None,
                            "context_text": context_text,
                            "page_url": page_url,
                            "warc_file": warc_file,
                            "segment": segment_name
                    })
                except:
                    continue
        return images

    def process_warc(self,file_name,segment_name):
        print("\n\nProcessing %s" % file_name)
        img_data = []
        warc_file = warc.open(file_name, 'rb')
        for i, record in enumerate(warc_file):
            url, doc = self.read_doc(record)
            if doc is None or url is None:
                continue
            img_data+=self.acquire_links(doc,url,file_name,segment_name)
        warc_file.close()
        print("\nFound %d images matching criteria\n" % len(img_data))
        return img_data

    def download_warc(self,base_url,path,dest):
        wget.download(urljoin(base_url,path),dest)

    def write_to_csv(self, data, file_name):
        print("\nWriting csv %s" % file_name)
        data_file = open(file_name, 'w')
        csv_writer = csv.writer(data_file)
        count = 0
        for data_point in data:
            if count == 0:
                header = data_point.keys()
                csv_writer.writerow(header)
            count += 1
            csv_writer.writerow(data_point.values())

    def download_images(self,links,folder):
        print("\nDownloading images in segment %s" % folder)
        downloader = ImgScrapy(folder)
        failed_imgs = downloader.download_images(links)
        return failed_imgs

    def create_dir(self,path):
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except:
                print("Directory not found")
