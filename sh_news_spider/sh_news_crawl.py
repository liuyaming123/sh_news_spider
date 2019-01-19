# -*-coding:utf-8-*-

'''python3 代码'''
import hashlib
import json
import os

import oss2
import requests
from PIL import Image
from lxml import etree,html
import time,random
from pymongo import MongoClient

# 导入配置文件
# conf_dir = os.sep.join(['conf','conf.json'])
conf_dir = os.sep.join(['conf','test_conf.json'])
f = open(conf_dir, 'r')
config = json.load(f)

# 日志文件地址，需要什么日志，添加相应路径即可
logfile_base_dir = config["logs_path"]
log_file_path = "{}/logs/".format(logfile_base_dir)

log_file_text = "{}/text_info.log".format(log_file_path)
log_file_imgs = "{}/imgs_resp.log".format(log_file_path)
log_file_error = "{}/error.log".format(log_file_path)


class NewsSpider():
    def __init__(self):
        self.auth = oss2.Auth(config["access_key_id"], config["access_key_secret"])
        conn = MongoClient(config["mongodb_conn"]["name"])
        self.sh_news_info = conn.toutiao_clean.toutiao_article    # 创建库表
        # self.sh_news_info = conn.news_crawl.sh_news_info  # 创建库表

    def cat_crawl(self):
        # 起始url
        url = 'http://www.sh.chinanews.com/shms/index.shtml'
        html0 = requests.get(url).text
        ele = etree.HTML(html0)
        text_li = ele.xpath('//li/div[@class="con_title"]/a/@href')[:]    # 新闻标题列表，共400个
        print(len(text_li))

        # text 为列表页信息
        cnt = 0
        cnt_error = 0
        li = []
        for tit in text_li:
            time.sleep(random.randint(1,5))
            cnt += 1
            url_l = 'http://www.sh.chinanews.com' + tit
            print(url_l)
            url_l_md5 = self.md5_key(url_l)

            # 增量爬
            # if self.sh_news_info.find_one({'sim_id': url_l_md5}):
            #     print('该网页已爬取...')
            #     continue

            res = requests.get(url_l)
            res.encoding = 'gbk'
            ele1 = etree.HTML(res.text)

            tree = html.fromstring(res.text)
            name = tree.xpath("//div[@class='cms-news-article-content-block']")

            try:
                article_title = ele1.xpath('//div[@class="cms-news-article-title"]/span/text()')[0]
                time_and_source = ' '.join(ele1.xpath('//div[@class="cms-news-article-title-source"]/text()')[0].split(' '))
                text_main = '    ' + ele1.xpath('.//div[@class="cms-news-article-content-block"]')[0].xpath('string(.)').strip()[:-1]
                text = html.tostring(name[0])[:-390].decode('utf-8')

                # 入库
                id = self.id_auto_increase(self.sh_news_info)
                self.sh_news_info.insert({'_id':id,'name':'SH_News','sim_id':url_l_md5,'url':url_l,'title':article_title,'time_and_source':time_and_source,'text':text,'digest':text_main[:200],'source':'上海新闻网','crawl_time':int(time.time()),'time':int(time.time()),'status':1})

                print(article_title)
                print(time_and_source)
                print('cnt',cnt)
                self.log("info",article_title+"\n"+time_and_source+"\n"+text+"\n",log_file_text)

                prefix1 = 'http://www.sh.chinanews.com/shms/'
                prefix2 = url_l.split('/')[-2] + '/'
                raw_l = ele1.xpath('//div[@class="article_pic"]/img/@src')

                img_urls = []
                rawest_ls = []
                for raw in raw_l:
                    rawest_ls.append(raw)
                    pic = prefix1 + prefix2 + raw
                    img_urls.append(pic)
                pic_more = ele1.xpath('//div[@class="img_wrapper"]/img/@src')
                # print('pic_more:',pic_more)
                rawest_ls = rawest_ls + pic_more if len(pic_more) > 0 else rawest_ls
                img_urls = img_urls + pic_more if len(pic_more) > 0 else img_urls
                print('img_urls:',img_urls)

                if img_urls:
                    aliyun_imgUrls = []
                    raw_imgUrls = []
                    for img_url in img_urls:
                        di = {}
                        di_a = {}
                        di['url'] = img_url
                        try:
                            res = self.download_image(img_url)
                            img_content = res.content
                            time.sleep(1)

                            d_cnt = 0
                            while not img_content:
                                d_cnt += 1
                                img_content = self.download_image(img_url) if d_cnt < 3 else None

                            if not img_content:
                                self.log("info",str(requests.get(img_url)),log_file_imgs)
                            iname = self.md5_key(img_url)

                            cname = iname + ".jpg"
                            key = config['oss_img_path'] + '/' + cname
                            aliyun_imgUrl = self.up_img(name=cname, key=key, data=res)
                            di_a['url'] = aliyun_imgUrl
                            print('aliyun_imgUrl:',aliyun_imgUrl)

                            file_path_grandfather = config["imgs_path"]

                            if not os.path.exists("{}/{}".format(file_path_grandfather,time.strftime('NewsImg_%Y-%m-%d', time.localtime()))):
                                os.makedirs("{}/{}".format(file_path_grandfather,time.strftime('NewsImg_%Y-%m-%d', time.localtime())))

                            with open("{}/{}/{}.jpg".format(file_path_grandfather,time.strftime('NewsImg_%Y-%m-%d', time.localtime()),iname), 'wb') as f:
                                f.write(img_content)

                            path = "{}/{}/{}.jpg".format(file_path_grandfather,time.strftime('NewsImg_%Y-%m-%d', time.localtime()),iname)
                            img = Image.open(path)
                            di['width'] = di_a['width'] = img.size[0]
                            di['height'] = di_a['height'] = img.size[1]
                            print('width:', img.size[0])
                            print('height:', img.size[1])
                            raw_imgUrls.append(di)
                            aliyun_imgUrls.append(di_a)

                        except Exception as e:
                            self.log("info", str(e) + ":" + str(url_l), log_file_error)
                            continue

                    for i in range(len(rawest_ls)):
                        print('rawest_ls:',rawest_ls)
                        print('aliyun_imgUrls:',aliyun_imgUrls)
                        text = text.replace('src="%s"'%rawest_ls[i],'src="%s"'%aliyun_imgUrls[i]['url'])

                    self.sh_news_info.update({'_id': id},{'$set': {'text':text,'raw_imgUrls': raw_imgUrls,'pics': aliyun_imgUrls, 'update_time': int(time.time())}},
                                    upsert=True)

                print(text)

            except Exception as e:
                cnt_error += 1
                li.append((cnt_error, e, url_l))
                self.log("info",str(e)+":"+str(url_l),log_file_error)
                continue

        # print('cnt_error:',cnt_error)
        # print('exce:',li)

    # 图片下载
    def download_image(self,img_url):
        res = requests.get(img_url)
        return res

    # 上传图片到 阿里云
    def up_img(self, name, key, data):
        bucket = oss2.Bucket(self.auth, endpoint=config["endpoint"], bucket_name=config["bucket_name"])
        bucket.put_object(key, data=data)
        path = 'http://mmb-toutiao.oss-cn-shanghai.aliyuncs.com/toutiaoImage/%s' % name
        return path

    # md5加密
    def md5_key(self, arg):
        hash = hashlib.md5()
        hash.update(arg.encode("utf-8"))
        return hash.hexdigest()

    # 日志
    def log(self,type="info", text="",log_file=""):
        if not os.path.isdir(log_file_path):
            os.makedirs(log_file_path)
        log = "[{}] [{}] {}".format(type, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), text)
        with open(log_file, "a+", encoding='utf-8') as f:
            f.write(log+"\n")


    def id_auto_increase(self, coll):
        try:
            id = int(coll.find().sort('_id', -1).limit(1)[0]['_id'])+1
        except Exception as e:
            print(e)
            print('The query sequence number( _id ) failed and the sequence number was inserted from scratch')
            id = 0
        return id



if __name__ == '__main__':
    spider = NewsSpider()
    spider.cat_crawl()

