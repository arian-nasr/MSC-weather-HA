import xml.etree.ElementTree as ET
import json
import logging
import paho.mqtt.client as mqtt
import re
import io
import os

logger = logging.getLogger(__name__)

BROKER = os.getenv("MQTT_BROKER", "192.168.2.17")
PORT = int(os.getenv("MQTT_PORT", 1883))
DISCOVERY_PREFIX = "homeassistant"
BASE_TOPIC = "weather/station"


def slugify(text):
    return re.sub(r'\W+', '_', text).lower() if text else "unknown_station"


def parse_weather_xml(source) -> dict | None:
    try:
        tree = ET.parse(io.BytesIO(source))
        root = tree.getroot()
        current = root.find('currentConditions')
        if current is None:
            return None

        wind_speed_str = current.findtext('wind/speed')
        if wind_speed_str == "calm":
            wind_speed_str = "0"

        return {
            "station_name": current.findtext('station'),
            "condition": current.findtext('condition'),
            "temperature": float(current.findtext('temperature') or 0),
            "relative_humidity": float(current.findtext('relativeHumidity') or 0),
            "pressure": float(current.findtext('pressure') or 0),
            "wind_speed": float(wind_speed_str or 0),
            "wind_bearing": float(current.findtext('wind/bearing') or 0),
        }
    except Exception as e:
        logger.error(f"XML Parse Error: {e}")
        return None


def setup_ha_discovery(client, data):
    station_id = slugify(data['station_name'])
    state_topic = f"{BASE_TOPIC}/{station_id}/state"

    sensors = [
        {
            "id": "cond",
            "name": "Condition",
            "class": None,
            "state_class": None,
            "unit": None,
            "key": "condition",
            "icon": "mdi:weather-partly-cloudy"  # Default icon
        },
        {
            "id": "temp",
            "name": "Temperature",
            "class": "temperature",
            "state_class": "measurement",
            "unit": "°C",
            "key": "temperature"
        },
        {
            "id": "hum",
            "name": "Humidity",
            "class": "humidity",
            "state_class": "measurement",
            "unit": "%",
            "key": "relative_humidity"
        },
        {
            "id": "pres",
            "name": "Pressure",
            "class": "atmospheric_pressure",
            "state_class": "measurement",
            "unit": "kPa",
            "key": "pressure"
        },
        {
            "id": "wind_s",
            "name": "Wind Speed",
            "class": "wind_speed",
            "state_class": "measurement",
            "unit": "km/h",
            "key": "wind_speed"
        },
        {
            "id": "wind_b",
            "name": "Wind Bearing",
            "class": "wind_direction",
            "state_class": "measurement_angle",
            "unit": "°",
            "key": "wind_bearing"
        },
    ]

    device_info = {
        "identifiers": [f"weather_station_{station_id}"],
        "name": data['station_name'],
        "model": "XML Weather Station",
        "manufacturer": "Python Script"
    }

    for s in sensors:
        discovery_topic = f"{DISCOVERY_PREFIX}/sensor/{station_id}/{s['id']}/config"

        payload = {
            "name": f"{data['station_name']} {s['name']}",
            "unique_id": f"{station_id}_{s['id']}",
            "state_topic": state_topic,
            "value_template": f"{{{{ value_json.{s['key']} }}}}",
            "device": device_info
        }

        # optionals
        if s.get('class'):
            payload["device_class"] = s['class']

        if s.get('state_class'):
            payload["state_class"] = s['state_class']

        if s.get('unit'):
            payload["unit_of_measurement"] = s['unit']

        if s.get('icon'):
            payload["icon"] = s['icon']

        client.publish(discovery_topic, json.dumps(payload), retain=True)

    return state_topic


def publish_weather(data):
    if not data:
        return

    client = mqtt.Client()
    try:
        client.connect(BROKER, PORT, 60)

        client.loop_start()

        state_topic = setup_ha_discovery(client, data)

        msg_info = client.publish(state_topic, json.dumps(data), retain=True)

        msg_info.wait_for_publish()

        logger.info(f"Published all fields for {data['station_name']} to {state_topic}")

    except Exception as e:
        logger.error(f"MQTT Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
