import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import requests
from confluent_kafka import Producer, KafkaException
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nyc311_producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
NYC_311_API_URL = 'https://data.cityofnewyork.us/resource/erm2-nwe9.json'

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
}
KAFKA_TOPIC = 'nyc311-service-requests'

# Data configuration
REQUIRED_FIELDS = [
    'unique_key', 'created_date', 'complaint_type',
    'incident_zip', 'status', 'agency'
]

class NYC311Producer:
    """Handler for producing NYC 311 service requests to Kafka."""
    
    def __init__(self, kafka_config: Dict[str, Any], topic: str):
        self.kafka_config = kafka_config
        self.topic = topic
        self.producer = self._create_kafka_producer()

    def _create_kafka_producer(self) -> Producer:
        try:
            return Producer(self.kafka_config)
        except KafkaException as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def fetch_recent_requests(self, minutes: int = 10080) -> List[Dict[str, Any]]:
        """
        Fetch recent 311 requests from the NYC Open Data API.
        Args:
            minutes: Number of minutes to look back for requests
        Returns:
            List of request dictionaries
        """
        time_threshold = datetime.now() - timedelta(minutes=minutes)
        logger.info(f"Querying for created_date > '{time_threshold.strftime('%Y-%m-%dT%H:%M:%S')}'")
        query = f"created_date > '{time_threshold.strftime('%Y-%m-%dT%H:%M:%S')}'"
        params = {
            '$where': query,
            '$select': 'unique_key,created_date,complaint_type,incident_zip,incident_address,status,agency,borough',
            '$limit': 10000,
            '$order': 'created_date DESC'
        }
        try:
            response = requests.get(NYC_311_API_URL, params=params)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            logger.error(f"API request failed: {e}")
            return []

    def process_and_publish(self, requests: List[Dict[str, Any]]) -> None:
        successful = 0
        failed = 0
        for request in requests:
            try:
                if self._validate_request(request):
                    self._enrich_request(request)
                    self._publish_request(request)
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Error processing request {request.get('unique_key', 'unknown')}: {e}")
                failed += 1
        logger.info(f"Published {successful} requests, failed {failed} requests")

    def _validate_request(self, request: Dict[str, Any]) -> bool:
        return all(field in request for field in REQUIRED_FIELDS)

    def _enrich_request(self, request: Dict[str, Any]) -> None:
        request.update({
            'processing_timestamp': datetime.now().isoformat(),
            'data_source': 'nyc_311_api',
            'pipeline_version': '1.0'
        })

    def _publish_request(self, request: Dict[str, Any]) -> None:
        try:
            self.producer.produce(self.topic, json.dumps(request).encode('utf-8'))
        except BufferError:
            logger.warning("Producer queue is full, flushing...")
            self.producer.flush()
            self.producer.produce(self.topic, json.dumps(request).encode('utf-8'))
        except KafkaException as e:
            logger.error(f"Failed to publish request {request.get('unique_key', 'unknown')}: {e}")
            raise

    def run(self) -> None:
        logger.info(f"Starting NYC 311 Service Requests Producer...")
        logger.info(f"Using API endpoint: {NYC_311_API_URL}")
        logger.info(f"Kafka topic: {self.topic}")
        try:
            while True:
                try:
                    requests = self.fetch_recent_requests()
                    if requests:
                        logger.info(f"Fetched {len(requests)} new requests")
                        self.process_and_publish(requests)
                        self.producer.flush()
                    else:
                        logger.info("No new requests found")
                    time.sleep(60)
                except Exception as e:
                    logger.error(f"Error in processing cycle: {e}")
                    time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        try:
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def main():
    try:
        producer = NYC311Producer(KAFKA_CONFIG, KAFKA_TOPIC)
        producer.run()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main() 