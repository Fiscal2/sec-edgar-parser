import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..core.exceptions import NetworkException
from ..core.config import settings

logger = logging.getLogger(__name__)

class RateLimitedSession:
    def __init__(self, requests_per_second: float = None, delay: float = None):
        self.requests_per_second = requests_per_second or settings.requests_per_second
        self.delay = delay or settings.delay_between_requests
        self.last_request_time = 0.0
        
        self.session = requests.Session()
        self._setup_retry_strategy()
        self._setup_headers()
    
    def _setup_retry_strategy(self) -> None:
        retry_strategy = Retry(
            total=settings.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _setup_headers(self) -> None:
        self.session.headers.update({
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
    
    def _rate_limit(self) -> None:
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.requests_per_second
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get(self, url: str, **kwargs) -> requests.Response:
        self._rate_limit()
        
        try:
            response = self.session.get(
                url, 
                timeout=settings.request_timeout,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise NetworkException(f"Failed to fetch {url}: {e}") from e
    
    def close(self) -> None:
        self.session.close()

class EdgarHttpClient:
    def __init__(self):
        self.session = RateLimitedSession()
    
    def get_filing_content(self, url: str) -> str:
        """Get the content of a filing from EDGAR"""
        response = self.session.get(url)
        return response.text
    
    def get_index_content(self, url: str) -> str:
        """Get the content of an index file from EDGAR"""
        response = self.session.get(url)
        return response.text
    
    def close(self) -> None:
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
