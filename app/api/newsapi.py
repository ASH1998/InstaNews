import requests
import json
import time
import os
import pickle
import pandas as pd
from omegaconf import OmegaConf


class NewsFetcher:
    def __init__(self, api_key_, rate_limit=5):
        self.api_key = api_key_
        self.rate_limit = rate_limit
        self.url = "https://eventregistry.org/api/v1/article/getArticles"
        self.headers = {'Content-Type': 'application/json'}

    def fetch_articles(self, keyword, page=1, count=10, sort_by="date", sort_asc=False):
        payload = json.dumps({
            "action": "getArticles",
            "keyword": keyword,
            "sourceLocationUri": [
                "https://en.wikipedia.org/wiki/United_States",
                "https://en.wikipedia.org/wiki/Canada",
                "https://en.wikipedia.org/wiki/United_Kingdom"
            ],
            "ignoreSourceGroupUri": "paywall/paywalled_sources",
            "articlesPage": page,
            "articlesCount": count,
            "articlesSortBy": sort_by,
            "articlesSortByAsc": sort_asc,
            "dataType": ["news", "pr"],
            "forceMaxDataTimeWindow": 31,
            "resultType": "articles",
            "apiKey": self.api_key
        })

        try:
            response = requests.request("POST", self.url, headers=self.headers, data=payload)
            response.raise_for_status()
            data = response.json()
            if 'articles' in data and 'results' in data['articles']:
                return data['articles']['results']
            else:
                print(f"No articles found for keyword: {keyword}")
                return []
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return []
        except Exception as err:
            print(f"Other error occurred: {err}")
            return []

    def fetch_articles_with_rate_limit(self, key_words):
        all_articles = []
        for i, keyword in enumerate(key_words):
            if i % self.rate_limit == 0 and i > 0:
                print("Rate limit reached, sleeping for 1 minute...")
                time.sleep(60)  # Sleep for 1 minute to respect rate limiting
            articles_bunch = self.fetch_articles(keyword)
            all_articles.extend(articles_bunch)
        return all_articles


class ArticleStorage:
    def __init__(self, storage_dir='news_data'):
        self.storage_dir = storage_dir
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def store_articles(self, articles_):
        for article in articles_:
            date_ = article['date']
            file_path = os.path.join(self.storage_dir, f"{date_}.pkl")
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    existing_articles = pickle.load(f)
                existing_articles.append(article)
                with open(file_path, 'wb') as f:
                    pickle.dump(existing_articles, f)
            else:
                with open(file_path, 'wb') as f:
                    pickle.dump([article], f)

    def get_articles(self, date_):
        file_path = os.path.join(self.storage_dir, f"{date_}.pkl")
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                articles_ = pickle.load(f)
            return pd.DataFrame(articles_)
        else:
            print(f"No articles found for date: {date_}")
            return pd.DataFrame()


# Example Usage
if __name__ == "__main__":
    config = OmegaConf.load('config.yaml')
    api_key = config.keys.newapi

    # Fetch articles
    fetcher = NewsFetcher(api_key)
    keywords = ["Tesla Inc", "SpaceX", "Artificial Intelligence", "Climate Change", "COVID-19"]
    articles = fetcher.fetch_articles_with_rate_limit(keywords)

    # Store articles
    storage = ArticleStorage()
    storage.store_articles(articles)

    # Get articles for a specific date
    date = "2024-10-31"
    df = storage.get_articles(date)
    print(df)