import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# ── Configuration ──────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'clickstream'
EVENTS_PER_SECOND = 5

# ── Sample E-Commerce Data ─────────────────────────────────
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones",  "category": "Electronics", "price": 79.99},
    {"id": "P002", "name": "Running Shoes",         "category": "Sports",      "price": 120.00},
    {"id": "P003", "name": "Coffee Maker",          "category": "Kitchen",     "price": 49.99},
    {"id": "P004", "name": "Yoga Mat",              "category": "Sports",      "price": 35.00},
    {"id": "P005", "name": "Laptop Stand",          "category": "Electronics", "price": 29.99},
    {"id": "P006", "name": "Water Bottle",          "category": "Sports",      "price": 18.99},
    {"id": "P007", "name": "Smart Watch",           "category": "Electronics", "price": 199.99},
    {"id": "P008", "name": "Python Book",           "category": "Books",       "price": 25.00},
    {"id": "P009", "name": "Desk Lamp",             "category": "Home",        "price": 42.00},
    {"id": "P010", "name": "Protein Powder",        "category": "Health",      "price": 55.00},
]

# page_view appears 3x to make it more frequent (realistic)
EVENT_TYPES  = ["page_view", "page_view", "page_view",
                "add_to_cart", "purchase", "search", "wishlist"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PAGES        = ["/home", "/category", "/product", "/cart", "/checkout", "/search"]
COUNTRIES    = ["USA", "UK", "Canada", "Germany", "France", "Australia", "India"]

# Track active sessions per user
active_sessions = {}


def get_or_create_session(user_id):
    if user_id not in active_sessions:
        active_sessions[user_id] = {
            "session_id": fake.uuid4()[:8],
            "device":     random.choice(DEVICE_TYPES),
            "country":    random.choice(COUNTRIES),
        }
    return active_sessions[user_id]


def generate_event():
    user_id    = f"U{random.randint(1, 200):04d}"
    session    = get_or_create_session(user_id)
    product    = random.choice(PRODUCTS)
    event_type = random.choice(EVENT_TYPES)
    revenue    = product["price"] * random.randint(1, 3) if event_type == "purchase" else 0.0

    return {
        "user_id":      user_id,
        "session_id":   session["session_id"],
        "event_type":   event_type,
        "product_id":   product["id"],
        "product_name": product["name"],
        "category":     product["category"],
        "price":        product["price"],
        "revenue":      revenue,
        "page_url":     random.choice(PAGES),
        "device_type":  session["device"],
        "country":      session["country"],
        "timestamp":    datetime.now().isoformat(),
    }


def main():
    print("=" * 50)
    print("  Clickstream Producer Started")
    print(f"  Topic : {TOPIC}")
    print(f"  Rate  : {EVENTS_PER_SECOND} events/second")
    print("  Press Ctrl+C to stop")
    print("=" * 50)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    count = 0
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            count += 1
            if count % 10 == 0:
                print(f"[{count:>6} events] {event['event_type']:<12} | "
                      f"User: {event['user_id']} | "
                      f"Device: {event['device_type']:<8} | "
                      f"Country: {event['country']}")
            time.sleep(1 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {count}")
        producer.close()


if __name__ == "__main__":
    main()