import scrapy
import time
import numpy as np

# Forum   > General Discussion > General Chat > Santai Others etc
# Level 0 > Level 1            > Level 3      > Level 4
# Level 0 = https://carigold.com/forum/

class CarigoldSpiderSpider(scrapy.Spider):
    name = "carigold_spider"
    allowed_domains = ["carigold.com"]
    start_urls = ["https://carigold.com/forum/forums/general-chat.174"]
    
    def parse(self, response):
        # parse L3 only for now, which is General Chat section
        all_L4_sections = response.css('.node-title a::attr(href)').getall()
        for sections in all_L4_sections:
            random_intervals = abs(np.random.random(1)[0] *3   + np.random.random(1)[0] * 4)
            # time.sleep(random_intervals)
            yield response.follow(sections, callback=self.parse_L4)
    
    def parse_L4(self, response):
        L4_threads = response.xpath("//div[@class = 'structItem-cell structItem-cell--main']")
        total_number_of_pages_in_L4 = response.css('.block-outer--after .pageNav-page--skipEnd+ .pageNav-page a::text').get() 

        # parse first page first
        for threads_ in L4_threads:
            thread_link = threads_.xpath("./div/a/@href[not(ancestor::a/@class='labelLink')]").get()

            # self.thread_title = threads_.css('.structItem-title a::text').get() ### dunno how to pass value from this one to another method

            yield response.follow(thread_link, callback=self.parse_thread)

        # parse 2nd page
        if total_number_of_pages_in_L4:
            #################################### set max scrape page
            for x in range(2,int(total_number_of_pages_in_L4[-1]) + 1):
            # for x in range(2,2):
                next_L4_url = f'{response.url}page-{x}'
                random_intervals_1 = abs(np.random.random(1)[0] *3)
                # time.sleep(random_intervals_1)
                yield scrapy.Request(url = next_L4_url, callback=self.parse_L4)
                
    def parse_thread(self, response):
        # Parse first page first
        first_page_of_thread_url = response.url

        yield scrapy.Request(first_page_of_thread_url, callback=self.parse_replies)
        
        # Loop if got 2nd page then parse, otherwise it will return None
        max_page_available = response.css('div.block-outer.block-outer--after li.pageNav-page a::text').getall()

        if max_page_available:
            #################################### set max scrape page
            for x in range(2,int(max_page_available[-1]) + 1):
            # for x in range(2,4):
                topic_url = f'{response.url}page-{x}'
                yield scrapy.Request(url = topic_url, callback=self.parse_replies)

    def parse_replies(self,response):
        list_of_replies = response.css('div.message-main article.message-body') 

        reply_text = dict()
        for replies in list_of_replies:
            pre_ad_replies = ''.join(replies.css('div.bbWrapper::text').extract())
            if pre_ad_replies.__contains__('lightbox_share'): pre_ad_replies = '' #remove some random adobe dreamweaver's lightbox?

            post_ad_replies = ''.join(replies.css('div.bbWrapper > div ::text').extract())
            if post_ad_replies.__contains__('lightbox_share'): post_ad_replies = '' #remove some random adobe dreamweaver's lightbox?
            
            topic_title = response.css('h1.p-title-value::text').get()
            L4_title = response.css('.p-breadcrumbs--parent:nth-child(1) li:nth-child(4) span::text').get()
            L3_title = response.css('.p-breadcrumbs--parent:nth-child(1) li:nth-child(3) span::text').get()
            L2_title = response.css('.p-breadcrumbs--parent:nth-child(1) li:nth-child(2) span::text').get()
            topic_page = response.css('.block-outer--after .pageNav-page--current a::text').get()

            if pre_ad_replies != '':
                yield { 'url': response.url
                       ,'subsection2_title' : L2_title
                       ,'subsection3_title' : L3_title
                       ,'subsection4_title': L4_title
                       ,'topic_title': topic_title
                       ,'topic_page' : topic_page
                       ,'text' : pre_ad_replies}

            if post_ad_replies != '':
                yield {'url': response.url
                       ,'subsection2_title' : L2_title
                       ,'subsection3_title': L3_title
                       ,'subsection4_title' :L4_title
                       ,'topic_title': topic_title
                       ,'topic_page' : topic_page
                       , 'text': post_ad_replies}