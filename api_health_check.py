import requests
import time
import logging

API_URL = 'https://data.cityofnewyork.us/resource/erm2-nwe9.json'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_health_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_api():
    try:
        response = requests.get(API_URL, params={'$limit': 1})
        logger.info(f"Status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Sample data: {data[0] if data else 'No data returned'}")
        else:
            logger.warning(f"Non-200 response: {response.text}")
    except Exception as e:
        logger.error(f"Error contacting API: {e}")

if __name__ == "__main__":
    logger.info("Starting API health check loop...")
    while True:
        check_api()
        time.sleep(60) 