import time
import json
import logging
import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration
API_URL = "https://api.binance.com/api/v3/trades"
SYMBOL = "BTCUSDT"
TOPIC = "raw_crypto_trades"
# FIX 1: Change localhost to 127.0.0.1 here in the global config
BOOTSTRAP_SERVERS = ['127.0.0.1:9092'] 
POLL_INTERVAL = 2  

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BinanceProducer:
    def __init__(self):
        # Using kafka-python-ng compliant producer
        self.producer = KafkaProducer(
            # FIX 2: Use lowercase 'bootstrap_servers' and pass the global variable
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Batching optimization for throughput
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip'
        )
        self.last_trade_id = 0
        self.session = self._init_session()

    def _init_session(self):
        """Initialize requests session with exponential backoff strategy."""
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def fetch_trades(self):
        """Fetch trades from Binance API with Rate Limit protection."""
        params = {'symbol': SYMBOL, 'limit': 1000}
        try:
            response = self.session.get(API_URL, params=params, timeout=5)
            
            # Intelligent Rate Limit Monitoring
            # Header format: x-mbx-used-weight-1m
            weight_used = int(response.headers.get('X-MBX-USED-WEIGHT-1M', 0))
            
            if weight_used > 5000:
                logger.warning(f"High API weight usage: {weight_used}/6000. Initiating backoff...")
                time.sleep(5) 

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.error("Rate limit exceeded (429). Mandatory sleep 60s.")
                time.sleep(60)
            elif response.status_code == 418:
                logger.critical("IP Banned (418). Halting process.")
                exit(1)
            else:
                logger.error(f"API Error {response.status_code}: {response.text}")
                
        except Exception as e:
            logger.error(f"Network failure: {e}")
        return None

    def run(self):
        logger.info(f"Starting producer for {SYMBOL} -> {TOPIC}")
        while True:
            start_ts = time.time()
            trades = self.fetch_trades()
            
            if trades:
                # Deduplication: The API returns overlapping windows.
                # We sort by ID and only emit strictly new trades.
                trades.sort(key=lambda x: x['id'])
                new_count = 0
                
                for trade in trades:
                    if trade['id'] > self.last_trade_id:
                        # Schema Normalization
                        msg = {
                            "trade_id": trade['id'],
                            "symbol": SYMBOL,
                            "price": float(trade['price']),
                            "quantity": float(trade['qty']),
                            "timestamp": trade['time'], # Critical: Event Time
                            "is_buyer_maker": trade['isBuyerMaker']
                        }
                        # Fast asynchronous send (Standard Mode)
                        self.producer.send(TOPIC, value=msg)
                        
                        self.last_trade_id = trade['id']
                        new_count += 1
                
                if new_count > 0:
                    logger.info(f"Produced {new_count} trades. Latest Trade ID: {self.last_trade_id}")

            # Precise Polling Loop: Account for network time in the sleep interval
            elapsed = time.time() - start_ts
            time.sleep(max(0, POLL_INTERVAL - elapsed))

if __name__ == "__main__":
    producer = BinanceProducer()
    producer.run()