"""
Web Scraping Tutorial dengan Login dan Redis Storage
Termasuk: Login authentication, interval delays, dan penyimpanan data ke Redis
"""

import requests
from bs4 import BeautifulSoup
import redis
import json
import time
import random
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WebScraperWithRedis:
    """
    Class untuk web scraping dengan fitur:
    - Login authentication
    - Random delays untuk menghindari deteksi
    - Penyimpanan data ke Redis
    - Error handling dan retry mechanism
    """
    
    def __init__(self, 
                 redis_host: str = 'localhost',
                 redis_port: int = 6379,
                 redis_db: int = 0,
                 min_delay: int = 2,
                 max_delay: int = 5):
        """
        Inisialisasi scraper
        
        Args:
            redis_host: Host Redis server
            redis_port: Port Redis server
            redis_db: Database Redis yang digunakan
            min_delay: Minimum delay antar request (detik)
            max_delay: Maximum delay antar request (detik)
        """
        # Setup Redis connection
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("✅ Berhasil terhubung ke Redis")
        except redis.ConnectionError as e:
            logger.error(f"❌ Gagal terhubung ke Redis: {e}")
            raise
        
        # Setup session dengan headers untuk menghindari deteksi
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.is_logged_in = False
    
    def random_delay(self):
        """Memberikan delay random untuk menghindari deteksi"""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.info(f"⏳ Menunggu {delay:.2f} detik...")
        time.sleep(delay)
    
    def login(self, login_url: str, credentials: Dict[str, str], 
              login_data_key: Optional[Dict[str, str]] = None) -> bool:
        """
        Login ke website
        
        Args:
            login_url: URL halaman login
            credentials: Dictionary berisi username dan password
                        contoh: {'username': 'user123', 'password': 'pass123'}
            login_data_key: Mapping nama field di form login
                           contoh: {'username': 'email', 'password': 'pwd'}
                           jika None, akan menggunakan key dari credentials
        
        Returns:
            bool: True jika login berhasil
        """
        try:
            logger.info("🔐 Memulai proses login...")
            
            # Ambil halaman login untuk mendapatkan CSRF token jika ada
            response = self.session.get(login_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Cari CSRF token jika ada
            csrf_token = None
            csrf_input = soup.find('input', {'name': ['csrf_token', '_csrf_token', 'csrf', '_token']})
            if csrf_input and hasattr(csrf_input, 'attrs'):
                token_value = csrf_input.attrs.get('value')  # type: ignore
                if token_value:
                    csrf_token = str(token_value) if not isinstance(token_value, list) else str(token_value[0])
                    logger.info("🔒 CSRF token ditemukan")
            
            # Siapkan data login
            if login_data_key:
                login_data = {
                    login_data_key.get('username', 'username'): credentials['username'],
                    login_data_key.get('password', 'password'): credentials['password']
                }
            else:
                login_data = credentials.copy()
            
            # Tambahkan CSRF token jika ada
            if csrf_token:
                login_data['csrf_token'] = str(csrf_token)
            
            # Random delay sebelum login
            self.random_delay()
            
            # Submit login form
            response = self.session.post(login_url, data=login_data)
            
            # Cek apakah login berhasil
            # Sesuaikan dengan indikator login sukses di website target
            if response.status_code == 200:
                # Bisa dicek dari redirect, cookie, atau konten halaman
                if 'logout' in response.text.lower() or 'dashboard' in response.text.lower():
                    self.is_logged_in = True
                    logger.info("✅ Login berhasil!")
                    return True
            
            logger.warning("⚠️ Login mungkin gagal, periksa kredensial")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error saat login: {e}")
            return False
    
    def scrape_page(self, url: str, parse_function=None) -> Optional[Dict]:
        """
        Scrape satu halaman
        
        Args:
            url: URL halaman yang akan di-scrape
            parse_function: Function custom untuk parsing halaman
                          Jika None, akan return raw HTML
        
        Returns:
            Dictionary berisi hasil scraping atau None jika gagal
        """
        try:
            logger.info(f"🔍 Scraping: {url}")
            
            # Random delay sebelum request
            self.random_delay()
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            if parse_function:
                return parse_function(response.content)
            else:
                return {
                    'url': url,
                    'status_code': response.status_code,
                    'html': response.text,
                    'timestamp': datetime.now().isoformat()
                }
                
        except requests.RequestException as e:
            logger.error(f"❌ Error saat scraping {url}: {e}")
            return None
    
    def scrape_multiple_pages(self, urls: List[str], 
                            parse_function=None,
                            save_to_redis: bool = True,
                            redis_key_prefix: str = 'scraped_data') -> List[Dict]:
        """
        Scrape multiple pages dengan delay
        
        Args:
            urls: List URL yang akan di-scrape
            parse_function: Function untuk parsing halaman
            save_to_redis: Simpan hasil ke Redis atau tidak
            redis_key_prefix: Prefix untuk key Redis
        
        Returns:
            List dictionary hasil scraping
        """
        results = []
        
        for idx, url in enumerate(urls, 1):
            logger.info(f"📄 Progress: {idx}/{len(urls)}")
            
            data = self.scrape_page(url, parse_function)
            
            if data:
                results.append(data)
                
                if save_to_redis:
                    # Simpan ke Redis dengan key unik
                    redis_key = f"{redis_key_prefix}:{idx}:{int(time.time())}"
                    self.save_to_redis(redis_key, data)
            
            # Delay tambahan setiap 10 request
            if idx % 10 == 0:
                extra_delay = random.uniform(5, 10)
                logger.info(f"⏸️ Break tambahan {extra_delay:.2f} detik setelah 10 request...")
                time.sleep(extra_delay)
        
        return results
    
    def save_to_redis(self, key: str, data: Dict, expiry: Optional[int] = None):
        """
        Simpan data ke Redis
        
        Args:
            key: Redis key
            data: Data yang akan disimpan (akan dikonversi ke JSON)
            expiry: Waktu expiry dalam detik (None = tidak expire)
        """
        try:
            json_data = json.dumps(data, ensure_ascii=False)
            
            if expiry:
                self.redis_client.setex(key, expiry, json_data)
            else:
                self.redis_client.set(key, json_data)
            
            logger.info(f"💾 Data disimpan ke Redis: {key}")
            
        except Exception as e:
            logger.error(f"❌ Error menyimpan ke Redis: {e}")
    
    def get_from_redis(self, key: str) -> Optional[Dict]:
        """
        Ambil data dari Redis
        
        Args:
            key: Redis key
        
        Returns:
            Dictionary data atau None jika tidak ditemukan
        """
        try:
            data = self.redis_client.get(key)
            if data:
                return json.loads(str(data))
            return None
        except Exception as e:
            logger.error(f"❌ Error mengambil dari Redis: {e}")
            return None
    
    def get_all_keys(self, pattern: str = '*') -> List[str]:
        """
        Ambil semua keys dari Redis dengan pattern tertentu
        
        Args:
            pattern: Pattern untuk matching keys (default: semua keys)
        
        Returns:
            List of keys
        """
        try:
            keys = self.redis_client.keys(pattern)  # type: ignore
            return [str(key) for key in keys] if keys else []  # type: ignore
        except Exception as e:
            logger.error(f"❌ Error mengambil keys: {e}")
            return []
    
    def save_batch_to_redis_list(self, list_key: str, data_list: List[Dict]):
        """
        Simpan batch data ke Redis List
        
        Args:
            list_key: Key untuk Redis list
            data_list: List data yang akan disimpan
        """
        try:
            for data in data_list:
                json_data = json.dumps(data, ensure_ascii=False)
                self.redis_client.rpush(list_key, json_data)
            
            logger.info(f"💾 {len(data_list)} data disimpan ke Redis list: {list_key}")
            
        except Exception as e:
            logger.error(f"❌ Error menyimpan batch ke Redis: {e}")
    
    def close(self):
        """Tutup koneksi"""
        self.session.close()
        logger.info("👋 Koneksi ditutup")


# ==================== CONTOH PENGGUNAAN ====================

def example_parse_function(html_content):
    """
    Contoh function untuk parsing HTML
    Sesuaikan dengan struktur website yang di-scrape
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Contoh: ambil semua judul artikel
    titles = [title.get_text().strip() for title in soup.find_all('h2', class_='article-title')]
    
    # Contoh: ambil semua paragraf
    paragraphs = [p.get_text().strip() for p in soup.find_all('p')]
    
    return {
        'titles': titles,
        'paragraphs': paragraphs[:5],  # Ambil 5 paragraf pertama
        'timestamp': datetime.now().isoformat()
    }


def main():
    """
    Contoh implementasi lengkap
    """
    
    # 1. Inisialisasi scraper
    scraper = WebScraperWithRedis(
        redis_host='localhost',
        redis_port=6379,
        redis_db=0,
        min_delay=2,  # Minimum 2 detik delay
        max_delay=5   # Maximum 5 detik delay
    )
    
    # 2. Login ke website (sesuaikan dengan website target)
    login_success = scraper.login(
        login_url='https://example.com/login',
        credentials={
            'username': 'your_username',
            'password': 'your_password'
        },
        login_data_key={
            'username': 'email',  # Jika form menggunakan 'email' bukan 'username'
            'password': 'pwd'      # Jika form menggunakan 'pwd' bukan 'password'
        }
    )
    
    if not login_success:
        logger.error("❌ Login gagal, proses dihentikan")
        return
    
    # 3. Daftar URL yang akan di-scrape
    urls_to_scrape = [
        'https://example.com/page1',
        'https://example.com/page2',
        'https://example.com/page3',
        # Tambahkan URL lainnya
    ]
    
    # 4. Scrape multiple pages dengan parse function custom
    results = scraper.scrape_multiple_pages(
        urls=urls_to_scrape,
        parse_function=example_parse_function,
        save_to_redis=True,
        redis_key_prefix='scraped_articles'
    )
    
    # 5. Simpan semua hasil dalam satu Redis list
    scraper.save_batch_to_redis_list('all_scraped_data', results)
    
    # 6. Contoh: Ambil data dari Redis
    logger.info("\n📥 Mengambil data dari Redis...")
    all_keys = scraper.get_all_keys('scraped_articles:*')
    logger.info(f"Total keys: {len(all_keys)}")
    
    if all_keys:
        # Ambil data pertama sebagai contoh
        first_data = scraper.get_from_redis(all_keys[0])
        logger.info(f"Contoh data: {json.dumps(first_data, indent=2, ensure_ascii=False)}")
    
    # 7. Tutup koneksi
    scraper.close()
    
    logger.info("✅ Proses scraping selesai!")


if __name__ == '__main__':
    """
    PERSIAPAN SEBELUM MENJALANKAN:
    
    1. Install dependencies:
       pip install requests beautifulsoup4 redis
    
    2. Install dan jalankan Redis:
       - Windows: Download dari https://github.com/microsoftarchive/redis/releases
       - Linux: sudo apt-get install redis-server
       - Mac: brew install redis
       
       Jalankan Redis server:
       - Windows: redis-server.exe
       - Linux/Mac: redis-server
    
    3. Sesuaikan parameter:
       - URL login
       - Credentials
       - URL yang akan di-scrape
       - Parse function sesuai struktur website target
    
    4. Jalankan script:
       python web_scraping_with_redis.py
    """
    
    main()
