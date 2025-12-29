#!/usr/bin/env python3
"""
advanced_crawler.py
Полный парсер: Async + Playwright + SQLite + Sitemap Support.
"""

import asyncio
import argparse
import hashlib
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Set, List, Optional
from urllib.parse import urlparse, urljoin

import aiohttp
import aiosqlite
import trafilatura
import yaml
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm.asyncio import tqdm
from yarl import URL

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("crawler.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Конфигурация по умолчанию ---
DEFAULT_CONFIG = {
    "user_agent": "AdvancedCrawler/2.1 (+https://github.com/your-repo)",
    "concurrency": 10,
    "timeout": 20,
    "delay": 0.3,
    "max_pages": 0,
    "restrict_domain": True,
    "use_playwright": False,
    "check_sitemap": True,  # Новая опция
    "output_dir": "data",
    "db_name": "crawler_state.db"
}

@dataclass
class ArticleData:
    url: str
    title: str
    text: str
    excerpt: str
    word_count: int
    content_hash: str
    author: Optional[str] = None
    date: Optional[str] = None
    tags: List[str] = field(default_factory=list)

# --- Утилиты ---
def normalize_url(url: str) -> str:
    try:
        u = URL(url)
        query = {k: v for k, v in u.query.items() if not k.startswith("utm_")}
        u = u.with_query(query).with_fragment(None)
        return str(u)
    except Exception:
        return url

def get_content_hash(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

# --- Менеджеры ---

class StorageManager:
    def __init__(self, db_path: str, output_dir: str):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.jsonl_path = self.output_dir / "output.jsonl"
        self.csv_path = self.output_dir / "compact.csv"

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            # Включаем WAL режим для лучшей конкурентности (чтобы дашборд мог читать)
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS visited (
                    url TEXT PRIMARY KEY,
                    content_hash TEXT,
                    status INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    word_count INTEGER DEFAULT 0
                )
            """)
            await db.commit()

    async def is_visited(self, url: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT 1 FROM visited WHERE url = ?", (url,))
            return await cursor.fetchone() is not None

    async def is_duplicate_content(self, content_hash: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT 1 FROM visited WHERE content_hash = ?", (content_hash,))
            return await cursor.fetchone() is not None

    async def mark_visited(self, url: str, content_hash: str = "", status: int = 200, word_count: int = 0):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO visited (url, content_hash, status, word_count) VALUES (?, ?, ?, ?)",
                (url, content_hash, status, word_count)
            )
            await db.commit()

    async def save_article(self, article: ArticleData):
        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(article), ensure_ascii=False) + "\n")
        
        safe_title = article.title.replace("\t", " ").replace("\n", " ")
        safe_exc = article.excerpt.replace("\t", " ").replace("\n", " ")
        with open(self.csv_path, "a", encoding="utf-8") as f:
            f.write(f"{safe_title}\t{article.url}\t{article.word_count}\t{safe_exc}\n")

class SitemapParser:
    """Обрабатывает sitemap.xml и sitemap index."""
    def __init__(self, session, base_domain: str):
        self.session = session
        self.base_domain = base_domain
        self.urls_found = set()

    async def fetch_and_parse(self, sitemap_url: str):
        logger.info(f"Checking sitemap: {sitemap_url}")
        try:
            async with self.session.get(sitemap_url) as resp:
                if resp.status != 200:
                    return
                content = await resp.read()
                
            soup = BeautifulSoup(content, "xml")
            
            # Проверка: это индекс или конечный файл?
            sitemap_tags = soup.find_all("sitemap")
            if sitemap_tags:
                # Это индекс - рекурсивно обходим
                tasks = []
                for sm in sitemap_tags:
                    loc = sm.find("loc")
                    if loc:
                        tasks.append(self.fetch_and_parse(loc.text.strip()))
                await asyncio.gather(*tasks)
            else:
                # Это конечный файл - собираем URL
                url_tags = soup.find_all("url")
                for u in url_tags:
                    loc = u.find("loc")
                    if loc:
                        link = normalize_url(loc.text.strip())
                        if self.base_domain in link: # Простая проверка домена
                            self.urls_found.add(link)
                            
        except Exception as e:
            logger.warning(f"Error parsing sitemap {sitemap_url}: {e}")

class PlaywrightManager:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None

    async def start(self):
        from playwright.async_api import async_playwright
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context(user_agent=DEFAULT_CONFIG['user_agent'])

    async def fetch_page(self, url: str, timeout: int) -> str:
        if not self.context:
            await self.start()
        page = await self.context.new_page()
        try:
            await page.goto(url, timeout=timeout * 1000, wait_until="domcontentloaded")
            await page.wait_for_timeout(1000)
            return await page.content()
        finally:
            await page.close()

    async def close(self):
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()

class ContentExtractor:
    @staticmethod
    def extract(html: str, url: str) -> tuple[Optional[ArticleData], Set[str]]:
        extracted = trafilatura.extract(
            html, url=url, include_links=True, output_format='json', with_metadata=True
        )
        
        links = set()
        soup = BeautifulSoup(html, "lxml")
        base_url = url
        base_tag = soup.find("base")
        if base_tag and base_tag.get("href"):
            base_url = base_tag.get("href")

        for a_tag in soup.find_all("a", href=True):
            full_url = urljoin(base_url, a_tag["href"])
            links.add(normalize_url(full_url))

        article = None
        if extracted:
            data = json.loads(extracted)
            text = data.get('text') or ""
            if len(text.split()) > 30: # Фильтр мусора
                content_hash = get_content_hash(text)
                article = ArticleData(
                    url=url,
                    title=data.get('title') or soup.title.string or "No Title",
                    text=text,
                    excerpt=text[:300].replace("\n", " "),
                    word_count=len(text.split()),
                    content_hash=content_hash,
                    author=data.get('author'),
                    date=data.get('date'),
                    tags=data.get('categories') or []
                )
        return article, links

class AsyncCrawler:
    def __init__(self, start_url: str, config: dict):
        self.start_url = normalize_url(start_url)
        self.base_domain = urlparse(self.start_url).netloc
        self.config = config
        self.queue = asyncio.Queue()
        self.storage = StorageManager(config['db_name'], config['output_dir'])
        self.pw_manager = PlaywrightManager() if config['use_playwright'] else None
        self.semaphore = asyncio.Semaphore(config['concurrency'])
        self.session = None
        self.pages_processed = 0
        self.running = True

    async def setup(self):
        await self.storage.init_db()
        
        timeout = aiohttp.ClientTimeout(total=self.config['timeout'])
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": self.config['user_agent']}
        )
        
        # 1. Sitemap logic
        if self.config.get('check_sitemap'):
            sitemap_url = urljoin(self.start_url, "/sitemap.xml")
            parser = SitemapParser(self.session, self.base_domain)
            await parser.fetch_and_parse(sitemap_url)
            
            added_count = 0
            for url in parser.urls_found:
                if not await self.storage.is_visited(url):
                    self.queue.put_nowait(url)
                    added_count += 1
            
            if added_count > 0:
                logger.info(f"Loaded {added_count} URLs from Sitemap.")
            else:
                logger.info("Sitemap not found or empty, starting from root.")
                self.queue.put_nowait(self.start_url)
        else:
            self.queue.put_nowait(self.start_url)

    async def close(self):
        if self.session: await self.session.close()
        if self.pw_manager: await self.pw_manager.close()

    @retry(retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)), stop=stop_after_attempt(3))
    async def fetch(self, url: str) -> str:
        if self.config['use_playwright']:
            return await self.pw_manager.fetch_page(url, self.config['timeout'])
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.text()

    async def process_url(self, url: str):
        async with self.semaphore:
            try:
                if self.config['restrict_domain'] and urlparse(url).netloc != self.base_domain:
                    return
                if await self.storage.is_visited(url):
                    return

                await asyncio.sleep(self.config['delay'])
                
                try:
                    html = await self.fetch(url)
                except Exception:
                    await self.storage.mark_visited(url, status=0)
                    return

                article, links = ContentExtractor.extract(html, url)
                
                # Добавление новых ссылок в очередь
                for link in links:
                    if self.config['restrict_domain'] and urlparse(link).netloc != self.base_domain:
                        continue
                    # Проверяем is_visited позже, но можем проверить наличие в очереди, 
                    # если очередь очень большая, лучше делать это здесь
                    if not await self.storage.is_visited(link):
                         await self.queue.put(link)

                status = 200
                word_count = 0
                chash = ""
                
                if article:
                    chash = article.content_hash
                    word_count = article.word_count
                    if not await self.storage.is_duplicate_content(chash):
                        await self.storage.save_article(article)
                        logger.info(f"[OK] {article.title[:30]}... ({word_count} w)")
                    else:
                        logger.info(f"[DUP] {url}")
                
                await self.storage.mark_visited(url, content_hash=chash, status=status, word_count=word_count)
                self.pages_processed += 1

            except Exception as e:
                logger.error(f"Error {url}: {e}")

    async def worker(self, pbar):
        while self.running:
            try:
                url = await self.queue.get()
                await self.process_url(url)
                self.queue.task_done()
                pbar.update(1)
                
                if self.config['max_pages'] and self.pages_processed >= self.config['max_pages']:
                    self.running = False
                    while not self.queue.empty():
                        try:
                            self.queue.get_nowait()
                            self.queue.task_done()
                        except asyncio.QueueEmpty: break
            except asyncio.CancelledError: break
            except Exception: pass

    async def run(self):
        await self.setup()
        pbar = tqdm(total=self.config['max_pages'] or 0, unit="pg")
        workers = [asyncio.create_task(self.worker(pbar)) for _ in range(self.config['concurrency'])]
        try:
            await self.queue.join()
        except KeyboardInterrupt:
            self.running = False
            for w in workers: w.cancel()
        await self.close()
        pbar.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", "-s", required=True)
    parser.add_argument("--config", "-c")
    parser.add_argument("--playwright", action="store_true")
    parser.add_argument("--no-sitemap", action="store_true", help="Skip sitemap check")
    
    args = parser.parse_args()
    config = DEFAULT_CONFIG.copy()
    
    if args.config and Path(args.config).exists():
        with open(args.config) as f: config.update(yaml.safe_load(f))
    
    if args.playwright: config['use_playwright'] = True
    if args.no_sitemap: config['check_sitemap'] = False
        
    asyncio.run(AsyncCrawler(args.start, config).run())

if __name__ == "__main__":
    main()