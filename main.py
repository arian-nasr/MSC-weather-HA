import amqp
import ssl
import sys
import logging
import requests
from weather_utility import parse_weather_xml, publish_weather

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

HOST = "dd.weather.gc.ca"
PORT = 5671
USER = "anonymous"
PASSWORD = "anonymous"
EXCHANGE = "xpublic"

QUEUE_NAME = "q_anonymous.subscribe.citypage.companyis2ari.ca"
SUBTOPIC = "#.WXO-DD.citypage_weather.ON.#"

def on_message(message):
    routing_key = message.delivery_info.get('routing_key')
    body = message.body.decode('utf-8') if isinstance(message.body, bytes) else message.body
    
    parts = body.split()

    clean_url = f"{parts[1].strip()}{parts[2].strip()}"

    if clean_url.endswith("s0000458_en.xml"):
        logger.debug(f"New Toronto weather update available at: {clean_url}")
        try:
            response = requests.get(clean_url)
            response.raise_for_status()
            weather_data = parse_weather_xml(response.content)
            publish_weather(weather_data)
        except Exception as e:
            logger.error(f"Failed to fetch or process weather data: {e}")
    elif clean_url.endswith("s0000305_en.xml"):
        logger.debug(f"New Kaladar weather update available at: {clean_url}")
        try:
            response = requests.get(clean_url)
            response.raise_for_status()
            weather_data = parse_weather_xml(response.content)
            publish_weather(weather_data)
        except Exception as e:
            logger.error(f"Failed to fetch or process weather data: {e}")
    else:
        logger.debug(f"Ignored URL (not Toronto weather): {clean_url}")

    
    message.channel.basic_ack(message.delivery_info['delivery_tag'])

def main():
    ssl_context = ssl.create_default_context()

    try:
        logger.debug(f"Connecting to {HOST}:{PORT} as {USER}")
        conn = amqp.Connection(
            host=f"{HOST}:{PORT}",
            userid=USER,
            password=PASSWORD,
            ssl=ssl_context,
            virtual_host="/"
        )
        conn.connect()
        channel = conn.channel()

        logger.debug(f"Declaring exclusive, auto-deleting queue: {QUEUE_NAME}")
        channel.queue_declare(
            queue=QUEUE_NAME,
            exclusive=True,
            auto_delete=True
        )

        logger.debug(f"Binding queue to exchange '{EXCHANGE}' with routing key '{SUBTOPIC}'")
        channel.queue_bind(
            queue=QUEUE_NAME,
            exchange=EXCHANGE,
            routing_key=SUBTOPIC
        )


        channel.basic_consume(queue=QUEUE_NAME, callback=on_message)

        logger.info("Waiting for Ontario weather updates...")
        while True:
            conn.drain_events()

    except KeyboardInterrupt:
        logger.info(f"Stopping script...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

