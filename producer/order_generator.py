"""
Bharatcommerce Intelligence Platform
order_generator.py — Kafka Producer

Run: python order_generator.py

Produces to topics:
  orders.raw       — every order event
  returns.raw      — ~20% of orders trigger a return
  anomalies.flagged — flagged events (high value, COD risk, bulk)

Partition key: state — so Maharashtra always lands on same partition.

Env vars (optional — defaults work for local Docker):
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  EVENTS_PER_SECOND         default: 3
  ANOMALY_INJECT_RATE       default: 4  (percent)
"""

import json
import logging
import os
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bc.producer")

# ── Geography ─────────────────────────────────────────────────
# (city, state, pincode_prefix, tier)
CITIES = [
    ("Mumbai",         "Maharashtra",   "400", 1),
    ("Delhi",          "Delhi",         "110", 1),
    ("Bengaluru",      "Karnataka",     "560", 1),
    ("Chennai",        "Tamil Nadu",    "600", 1),
    ("Hyderabad",      "Telangana",     "500", 1),
    ("Kolkata",        "West Bengal",   "700", 1),
    ("Pune",           "Maharashtra",   "411", 2),
    ("Ahmedabad",      "Gujarat",       "380", 2),
    ("Jaipur",         "Rajasthan",     "302", 2),
    ("Lucknow",        "Uttar Pradesh", "226", 2),
    ("Surat",          "Gujarat",       "395", 2),
    ("Indore",         "Madhya Pradesh","452", 2),
    ("Patna",          "Bihar",         "800", 3),
    ("Varanasi",       "Uttar Pradesh", "221", 3),
    ("Agra",           "Uttar Pradesh", "282", 3),
    ("Coimbatore",     "Tamil Nadu",    "641", 3),
    ("Bhopal",         "Madhya Pradesh","462", 3),
    ("Meerut",         "Uttar Pradesh", "250", 3),
    ("Nashik",         "Maharashtra",   "422", 3),
    ("Ranchi",         "Jharkhand",     "834", 3),
    ("Guwahati",       "Assam",         "781", 3),
    ("Jodhpur",        "Rajasthan",     "342", 3),
    ("Raipur",         "Chhattisgarh",  "492", 3),
    ("Kanpur",         "Uttar Pradesh", "208", 3),
    ("Rajkot",         "Gujarat",       "360", 3),
]

# Tier 3 cities twice as likely — matches Meesho's real user base
CITY_WEIGHTS = [1.0 if t == 1 else (1.5 if t == 2 else 2.5) for _, _, _, t in CITIES]

# ── Products ──────────────────────────────────────────────────
# (category, subcategory, min_price, max_price, return_rate_pct)
PRODUCTS = [
    ("Fashion",     "Kurti",            199,   1499, 42),
    ("Fashion",     "Saree",            399,   3999, 38),
    ("Fashion",     "Men's T-shirt",    199,    999, 35),
    ("Fashion",     "Jeans",            499,   2499, 40),
    ("Fashion",     "Lehenga",          799,   7999, 45),
    ("Electronics", "Mobile Phone",    4999,  24999,  8),
    ("Electronics", "TWS Earbuds",      499,   3999,  6),
    ("Electronics", "Power Bank",       599,   2499,  5),
    ("Electronics", "Smart Watch",      999,   7999,  9),
    ("Home",        "Steel Cookware",   299,   2999, 12),
    ("Home",        "Bedsheet Set",     299,   1999, 18),
    ("Beauty",      "Skincare Combo",   299,   1999, 15),
    ("Beauty",      "Hair Oil",          99,    599,  8),
    ("Books",       "Novel",             99,    599,  3),
    ("Grocery",     "Spices Combo",      99,    499,  5),
]

FIRST_NAMES = [
    "Aarav","Priya","Rahul","Ananya","Rohit","Deepa","Vikas","Sunita",
    "Kiran","Meera","Arjun","Kavya","Suresh","Divya","Amit","Neha",
    "Rajesh","Pooja","Vikram","Anjali","Kartik","Tanvi","Ishaan","Nisha",
]
LAST_NAMES = [
    "Sharma","Verma","Patel","Singh","Kumar","Gupta","Joshi","Mishra",
    "Yadav","Shah","Mehta","Reddy","Nair","Iyer","Rao","Pandey",
    "Chopra","Malhotra","Kapoor","Agarwal","Bansal","Tiwari","Dubey",
]

WAREHOUSES = ["WH-MUM-01","WH-DEL-01","WH-BLR-01","WH-HYD-01","WH-KOL-01"]

RETURN_REASONS = {
    "Fashion":     ["size_issue","size_issue","quality","wrong_item","changed_mind"],
    "Electronics": ["wrong_item","damaged","quality","changed_mind"],
    "Home":        ["quality","damaged","wrong_item","changed_mind"],
    "Beauty":      ["quality","wrong_item","changed_mind"],
    "Books":       ["wrong_item","damaged","changed_mind"],
    "Grocery":     ["damaged","quality","wrong_item"],
}


def payment_method(tier: int) -> str:
    weights = {
        1: [35, 40, 18, 7],
        2: [50, 35, 10, 5],
        3: [65, 25,  7, 3],
    }
    return random.choices(["COD","UPI","Card","Wallet"], weights=weights[tier])[0]


# ── Event dataclasses ─────────────────────────────────────────

@dataclass
class OrderEvent:
    order_id:            str
    event_timestamp:     str
    customer_id:         str
    customer_name:       str
    customer_tier:       int
    city:                str
    state:               str
    pincode:             str
    category:            str
    subcategory:         str
    quantity:            int
    unit_price_inr:      float
    total_amount_inr:    float
    payment_method:      str
    warehouse_id:        str
    estimated_days:      int
    is_express:          bool
    expected_return_rate:float
    is_anomalous:        bool
    anomaly_reason:      str
    ingestion_id:        str


@dataclass
class ReturnEvent:
    return_id:        str
    order_id:         str
    event_timestamp:  str
    customer_id:      str
    city:             str
    state:            str
    category:         str
    return_reason:    str
    refund_amount_inr:float
    refund_method:    str
    ingestion_id:     str


# ── Generators ────────────────────────────────────────────────

def make_order(inject_anomaly: bool = False) -> OrderEvent:
    city, state, pin_prefix, tier = random.choices(CITIES, weights=CITY_WEIGHTS)[0]
    pincode = f"{pin_prefix}{random.randint(100, 999):03d}"
    cat, subcat, pmin, pmax, ret_rate = random.choice(PRODUCTS)

    price   = round(random.uniform(pmin, pmax), 2)
    qty     = random.randint(1, 3)
    total   = round(price * qty, 2)
    pmnt    = payment_method(tier)
    anomaly = ""

    if inject_anomaly:
        atype = random.choice(["high_value", "cod_high_value", "bulk_qty"])
        if atype == "high_value":
            total   = round(random.uniform(20000, 99999), 2)
            anomaly = f"order_value_Rs{total:.0f}_exceeds_3sigma"
        elif atype == "cod_high_value":
            total   = round(random.uniform(6000, 20000), 2)
            pmnt    = "COD"
            anomaly = f"cod_Rs{total:.0f}_exceeds_Rs5000_threshold"
        elif atype == "bulk_qty":
            qty     = random.randint(20, 50)
            total   = round(price * qty, 2)
            anomaly = f"qty_{qty}_unusual_for_residential_pincode"

    return OrderEvent(
        order_id            = f"ORD-{uuid.uuid4().hex[:10].upper()}",
        event_timestamp     = datetime.now(tz=timezone.utc).isoformat(),
        customer_id         = f"CUST-{random.randint(1000000, 9999999)}",
        customer_name       = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        customer_tier       = tier,
        city                = city,
        state               = state,
        pincode             = pincode,
        category            = cat,
        subcategory         = subcat,
        quantity            = qty,
        unit_price_inr      = price,
        total_amount_inr    = total,
        payment_method      = pmnt,
        warehouse_id        = random.choice(WAREHOUSES),
        estimated_days      = {1: random.randint(1,3), 2: random.randint(2,5), 3: random.randint(3,8)}[tier],
        is_express          = random.random() < 0.12,
        expected_return_rate= ret_rate / 100,
        is_anomalous        = inject_anomaly,
        anomaly_reason      = anomaly,
        ingestion_id        = str(uuid.uuid4()),
    )


def make_return(order: OrderEvent) -> ReturnEvent:
    reasons = RETURN_REASONS.get(order.category, ["changed_mind"])
    return ReturnEvent(
        return_id         = f"RET-{uuid.uuid4().hex[:8].upper()}",
        order_id          = order.order_id,
        event_timestamp   = datetime.now(tz=timezone.utc).isoformat(),
        customer_id       = order.customer_id,
        city              = order.city,
        state             = order.state,
        category          = order.category,
        return_reason     = random.choice(reasons),
        refund_amount_inr = order.total_amount_inr,
        refund_method     = "wallet_credit" if order.payment_method == "COD" else "same_as_payment",
        ingestion_id      = str(uuid.uuid4()),
    )


# ── Kafka ─────────────────────────────────────────────────────

def on_delivery(err, msg):
    if err:
        log.error("Delivery failed [%s]: %s", msg.topic(), err)


def build_producer() -> Producer:
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    log.info("Connecting to Kafka at %s", servers)
    return Producer({
        "bootstrap.servers":  servers,
        "linger.ms":          5,
        "batch.size":         32768,
        "enable.idempotence": True,
        "acks":               "all",
        "retries":            5,
        "compression.type":   "snappy",
    })


def run():
    producer = build_producer()
    tps          = float(os.getenv("EVENTS_PER_SECOND", "3"))
    anomaly_rate = int(os.getenv("ANOMALY_INJECT_RATE", "4"))
    delay        = 1.0 / tps

    log.info("Producer running | %.1f events/sec | %d%% anomaly rate", tps, anomaly_rate)
    log.info("Press Ctrl+C to stop")

    produced = returns = anomalies = 0
    start    = time.monotonic()

    try:
        while True:
            inject = random.randint(1, 100) <= anomaly_rate
            order  = make_order(inject_anomaly=inject)

            producer.produce(
                topic       = "orders.raw",
                key         = order.state.encode(),
                value       = json.dumps(asdict(order)).encode(),
                on_delivery = on_delivery,
            )
            produced += 1

            # Return events (~20% rate, weighted by category return rate)
            if random.random() < order.expected_return_rate * 0.5:
                ret = make_return(order)
                producer.produce(
                    topic       = "returns.raw",
                    key         = order.state.encode(),
                    value       = json.dumps(asdict(ret)).encode(),
                    on_delivery = on_delivery,
                )
                returns += 1

            # Anomalies go to dedicated topic too
            if order.is_anomalous:
                producer.produce(
                    topic = "anomalies.flagged",
                    key   = order.order_id.encode(),
                    value = json.dumps({
                        "order_id":       order.order_id,
                        "reason":         order.anomaly_reason,
                        "city":           order.city,
                        "state":          order.state,
                        "amount_inr":     order.total_amount_inr,
                        "payment_method": order.payment_method,
                        "flagged_at":     datetime.now(tz=timezone.utc).isoformat(),
                    }).encode(),
                    on_delivery = on_delivery,
                )
                anomalies += 1
                log.warning("ANOMALY  %s | %s | Rs%.0f",
                            order.order_id, order.anomaly_reason, order.total_amount_inr)

            producer.poll(0)

            if produced % 50 == 0:
                elapsed = time.monotonic() - start
                log.info(
                    "Produced %4d orders | %3d returns | %2d anomalies | %.1f eps | last: %s, %s, Rs%.0f, %s",
                    produced, returns, anomalies,
                    produced / elapsed,
                    order.city, order.category,
                    order.total_amount_inr, order.payment_method,
                )

            time.sleep(delay)

    except KeyboardInterrupt:
        log.info("Stopping producer...")
    finally:
        producer.flush(30)
        elapsed = time.monotonic() - start
        log.info("Done. %d orders | %d returns | %d anomalies | %.1fs",
                 produced, returns, anomalies, elapsed)


if __name__ == "__main__":
    run()