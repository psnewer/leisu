import scrapy
import re
import sqlite3
import os
import pandas as pd
from scrapy_playwright.page import PageMethod
from datetime import datetime
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
from scrapy.exceptions import CloseSpider

conn = sqlite3.connect(os.path.split(os.path.realpath(__file__))[0]+'/../../src/db/tennis.db')

class AtpSpider(scrapy.Spider):
    name = 'atp_spider'
    allowed_domains = ['flashscore.com']
    start_urls = ['https://www.flashscore.com/tennis/']
    custom_settings = {
        'Referer':'https://www.flashscore.com/',
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        "DOWNLOADER_MIDDLEWARES": {
            "leisu.middlewares.RandomProxyMiddleware": 543,
        },
        "ITEM_PIPELINES": {
            "leisu.pipelines.ATPPipeline": 300,
        },
        "CONCURRENT_REQUESTS" : 3,
        "DOWNLOAD_DELAY" : 1,
        # "DOWNLOADER_MIDDLEWARES" : {
        #     'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
        # },
        "RETRY_ENABLED" : False ,
        # "RETRY_HTTP_CODES" : [403, 404],
        # "EXTENSIONS" : {
        #     'leisu.extensions.InactivityAndErrorMonitor': 500,
        # },
        # "INACTIVITY_TIMEOUT" : 60,
        "CLOSESPIDER_ERRORCOUNT" : 2,
        "CLOSESPIDER_TIMEOUT": 200,
        "JOBDIR": "./crawl_state",  # 启用断点续爬
        "LOG_LEVEL": "INFO",
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',  # 可选值：chromium / firefox / webkit
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
        },
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={
                    "playwright": True,  # 启用 Playwright
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "span.lmc__elementName")  # 等待渲染后的元素
                    ],
                },
            )

    def parse(self, response):
    # 找到 <span class="lmc__elementName">ATP - Singles</span> 并点击
        atp_link = response.css('span.lmc__elementName:contains("ATP - Singles")')
        if atp_link:
            # 触发点击
            self.logger.info("找到 'ATP - Singles' 链接，准备点击")
            yield scrapy.Request(
                response.url,
                callback=self.after_click,
                dont_filter=True,  # 禁用重复过滤
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("click", "span.lmc__elementName >> text=ATP - Singles"),
                        # 等待其父元素同级下出现 <span class="lmc__template">
                        PageMethod("wait_for_selector", "span.lmc__template")
                    ],
                },
            )
        else:
            self.logger.error("未找到 'ATP - Singles' 链接")

    def after_click(self, response):
        # 查找所有 <span class="lmc__template"> 元素
        templates = response.css('span.lmc__template')
        self.logger.info("找到 %d 个 lmc__template 元素", len(templates))

        for template in templates:
            # 在每个 <span class="lmc__template"> 元素中查找 <a> 标签
            a_tag = template.css('a')
            if a_tag:
                # 获取 <a> 标签的 href 属性值
                tour = a_tag.xpath('normalize-space(text())').get()
                # if (not tour.startswith('A')):
                #     continue
                # 拼接成完整 URL
                full_url = response.urljoin(a_tag.xpath('./@href').get())
                self.logger.info("找到 tour：%s, URL: %s", tour, full_url)

                # 传递给 parse_tour 进一步解析
                yield scrapy.Request(full_url, callback=self.parse_tour, meta={"tour": tour})

    def parse_tour(self, response):
        tour = response.meta.get("tour")
        # 在 tour 页面中，寻找文本包含 “Archive” 的 a 标签
        archive_href = response.xpath("//a[contains(text(),'Archive')]/@href").get()
        if archive_href:
            archive_url = "https://www.flashscore.com" + archive_href if archive_href.startswith("/") else archive_href
            self.logger.info(f"找到 Archive 链接：{archive_url}")
            # 若 Archive 页面为动态加载页面，则利用 playwright 等待 .archive_row 出现
            yield scrapy.Request(
                archive_url,
                callback=self.parse_archive,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", ".archive__row")
                    ],
                'tour': tour
                }
            )
        else:
            self.logger.info("在 tour 页面中未找到 Archive 链接。")

    def parse_archive(self, response):
        tour = response.meta.get("tour")
        # 在新页面中，查找所有 class 为 archive_row 的 div
        archive_rows = response.css("div.archive__row")
        for row in archive_rows:
            # 在每个 archive_row 内，查找 class 为 archive_season 的 div
            season_div = row.css("div.archive__season")
            a_tag = season_div.css("a")
            season = a_tag.xpath("text()").get()  # 取 a 标签文本，赋值给 season
            if (re.search(r'\d{4}', season)):
                season = re.search(r'\d{4}', season).group(0)
            if (int(season) < 2023):
                continue
            href = a_tag.xpath("@href").get()      # 取 a 标签的 href 属性
            if href:
                # 组合 URL，这里确保 href 为相对路径时拼接 www.flashscore.com
                full_url = "https://www.flashscore.com" + href if href.startswith("/") else href
                self.logger.info(f"赛季：{season} 对应链接：{full_url}")
                # 发起对赛季页面的请求，传递 season 信息
                yield scrapy.Request(full_url, callback=self.parse_season, dont_filter=True, meta={"playwright": True, "playwright_include_page": True, "season": season, "tour": tour})
            else:
                self.logger.info("未在 archive_row 中找到有效的 season 链接。")

    async def parse_season(self, response):
        tour = response.meta.get("tour")
        season = response.meta.get("season")
        self.logger.info(f"正在解析赛季数据：{season}")
        # 从 meta 中获取 playwright 页面对象
        page = response.meta["playwright_page"]

        # 循环点击 “Show more matches” 按钮，直到页面中不再出现
        while True:
            # 使用 Playwright 的选择器查找包含 “Show more matches” 文本的 a 标签
            show_more = await page.query_selector("a:has-text('Show more matches')")
            if not show_more:
                self.logger.info("不再存在 'Show more matches' 按钮，退出点击循环。")
                break
            self.logger.info("点击 'Show more matches' 按钮...")
            await show_more.click()
            await page.wait_for_timeout(10000)
            # 等待新内容加载，可根据实际情况调整等待时间

        await page.wait_for_selector("div.event__match")
        # 获取页面最新加载的 HTML 内容
        content = await page.content()
        # 使用 Scrapy 的 response.replace 方法构造新的 Response 对象进行解析
        new_response = response.replace(body=content)
        # 关闭 Playwright 页面以释放资源
        await page.close()

        field = new_response.xpath("//div[@class='event__titleBox']//a[@aria-label]/@aria-label").get()
        field = field.split(",", 1)[1].strip() if len(field.split(",", 1)) > 1 else 'hard'

        # 遍历页面中所有 class 包含 event__match 的 div
        match_divs = new_response.css("div[class*='event__match']")
        for match_div in match_divs:
            # 从每个 div 内查找 a 标签的 href 属性
            a_href = match_div.css("a::attr(href)").get()
            if a_href:
                # 拼接完整的比赛链接
                match_url = new_response.urljoin(a_href) + '/point-by-point'
                match_id = match_div.css("a::attr(aria-describedby)").get()
                sql_str = "SELECT * FROM tennis WHERE match_id = '%s'"%match_id
                df = pd.read_sql_query(sql_str,conn)
                if not df.empty and df.at[0, 'home'] != df.at[0, 'away']:
                    continue
                self.logger.info(f"发现比赛链接: {match_url}")
                # 交由 parse_match 进行后续解析，并传递当前赛季信息
                try:
                    yield response.follow(match_url, callback=self.parse_match, dont_filter=True, meta={"season": season, "tour": tour, "field": field, "match_id": match_id,
                                                                                    "playwright": True,
                                                                                    "playwright_include_page": True,
                                                                                    "playwright_page_methods": [
                                                                                            PageMethod("wait_for_selector", "button:has-text('Point by Point')"),
                                                                                        ],})
                except PlaywrightTimeoutError as e:
                    self.logger.error(f"Playwright Timeout Error: {e}")
                    self.crawler.engine.close_spider(self, reason=f"Page wait timeout exceeded: {e}")
                    raise CloseSpider(f"Playwright page wait timeout: {e}")

    async def parse_match(self, response):
        tour = response.meta.get("tour")
        season = response.meta.get("season")
        field = response.meta.get("field")
        match_id = response.meta.get("match_id")
        page = response.meta.get("playwright_page")
        self.logger.info(f"正在解析比赛页面：{response.url}")

        # # 找到并点击文本为 "Point by Point" 的 button
        # pb_button = await page.query_selector("button:has-text('Point by Point')")
        # if not pb_button:
        #     self.logger.info("未找到 'Point by Point' 按钮")
        #     await page.close()
        #     return
        # await pb_button.click()
        # self.logger.info("点击 'Point by Point' 按钮，等待新页面加载...")
        # 等待新页面中出现 duelParticipant__startTime 相关元素
        await page.wait_for_selector("div[class*='duelParticipant__startTime']")
        new_html = await page.content()
        new_response = response.replace(body=new_html)

        # 提取 date：从 class 包含 duelParticipant__startTime 的 div 中获取文本
        date = new_response.css("div[class*='duelParticipant__startTime'] *::text").get()
        date = datetime.strptime(date, "%d.%m.%Y %H:%M").strftime("%Y%m%d%H%M")
        self.logger.info(f"比赛日期：{date}")

        # 提取 home 与 away：取 class 包含 participant__participantName 的 a 标签，第一个为 home，第二个为 away
        participant_as = new_response.css("div[class*='participant__participantNameWrapper']")
        home = participant_as[0].css("a::text").get() if len(participant_as) >= 1 else None
        away = participant_as[1].css("a::text").get() if len(participant_as) >= 2 else None
        self.logger.info(f"主队：{home}，客队：{away}")

        score_spans = response.css("div.detailScore__wrapper span::text").getall()

        home_score = score_spans[0] if len(score_spans) == 3 else ''
        away_score = score_spans[2] if len(score_spans) == 3 else ''

        # 找到所有 title 属性中包含 "Set" 的 a 标签
        # 为避免因 DOM 变化导致的 stale element 问题，此处采用循环点击，每次重新查询未点击的按钮
        await page.wait_for_selector("a[title*='Set']", timeout=10000)
        sets_results = []
        # 获取所有 title 含有 "Set" 的 <a> 标签
        set_links = await page.query_selector_all("a[title*='Set']")

        if not set_links:
            self.logger.info("未找到任何 Set 按钮，直接返回。")
        else:
            for set_link in set_links:
                # 在 <a> 内查找 <button>
                set_button = await set_link.query_selector("button")
                if set_button:
                    # 获取 Set 的 title，如 "Set 1", "Set 2"
                    set_title = await set_link.get_attribute("title")
                    self.logger.info(f"点击 {set_title} 按钮...")
                    
                    # 点击该 Set 按钮
                    await set_button.click()
                    
                    # 等待 matchHistoryRow 加载
                    await page.wait_for_selector("div.matchHistoryRow")
                    
                    # 获取最新的页面 HTML 内容
                    set_html = await page.content()
                    set_response = response.replace(body=set_html)
                    sets_result = []
                    # 遍历 matchHistoryRow 里的数据
                    rows = set_response.css("div[class='matchHistoryRow']")
                    for row in rows:
                        servs = row.css("div[class*='matchHistoryRow__servis']")
                        service = '2'
                        for serv in servs:
                            side = serv.attrib.get("class","")
                            if 'home' in side and bool(serv.xpath('.//div[@title="Serving player"]')):
                                service = '0'
                                break
                            elif 'away' in side and bool(serv.xpath('.//div[@title="Serving player"]')):
                                service = '1'
                                break
                        score_divs = row.css("div[class*='matchHistoryRow__scoreBox']")
                        score_values = score_divs.xpath("./div[contains(@class, 'matchHistoryRow__score')]")
                        if not score_values:
                            score_values = score_divs.xpath("span")
                        if len(score_values) >= 2:
                            score_home = score_values[0].css("::text").get()
                            score_away = score_values[1].css("::text").get()
                            sets_result.append(score_home + '-' +score_away+ '-' + service)
                        if service == '2':
                            break
                    
                    # 等待一小段时间，避免过快点击
                    sets_results.append(sets_result)
                    await page.wait_for_timeout(200)

        # 将解析到的数据 yield 出来
        yield {
            "tour": tour,
            "field": field,
            "season": season,
            "match_id": match_id,
            "date": date,
            "home": home,
            "away": away,
            "home_score": home_score,
            "away_score": away_score,
            "sets": sets_results
        }

        # 关闭 playwright 页面释放资源
        await page.close()
