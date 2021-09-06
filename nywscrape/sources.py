

class NewsSource:
    def __init__(self):
        self.source_matcher = "https?://(www.)?theblaze.com"
        self.article_scraping_urls = ["https://theblaze.com/news"]
        self.article_matcher = ".*"

    def get