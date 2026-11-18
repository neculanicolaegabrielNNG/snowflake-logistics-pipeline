#!/usr/bin/env python3
"""Synthetic logistics data generator (Python standard library only).

Simulates a Romanian parcel-delivery network: 10 hubs, a fleet, drivers,
B2B customers (senders), shipments and the scan events produced while each
parcel moves through the network.

The data is *synthetic* but deliberately behaves like the real thing:

* weekly seasonality (almost no Sunday volume) and a Black Friday / Christmas peak
* hubs have a finite daily capacity -> volume above capacity causes delays
* cash-on-delivery (COD / "ramburs") parcels fail more often
* peripheral destinations fail more often than urban ones
* up to 3 delivery attempts, then the parcel is returned to the sender
* winter disruptions on the linehaul network

It also injects a few *intentional data quality issues* so the downstream
pipeline has something real to clean:

* ~0.4 % of events are duplicated (same event_id twice in the same file)
* ~0.3 % of events arrive late (written to a file 1-3 days after they happened)

Usage:
    python3 data_generator/generate_data.py --out data/raw
    python3 data_generator/generate_data.py --days 30 --base-volume 200 --seed 7
"""
from __future__ import annotations

import argparse
import csv
import math
import random
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

TS_FMT = "%Y-%m-%d %H:%M:%S"

# code, city, county, region, lat, lon, share of national volume (used to pick
# destinations and the home hub of each customer), utilisation on a normal day
# (capacity = expected volume ORIGINATING at the hub / utilisation, see size_hub_capacity)
HUB_SPECS = [
    ("BUC", "București", "București", "Muntenia", 44.4268, 26.1025, 0.36, 0.65),
    ("CLJ", "Cluj-Napoca", "Cluj", "Transilvania", 46.7712, 23.6236, 0.11, 0.75),
    ("TIM", "Timișoara", "Timiș", "Banat", 45.7489, 21.2087, 0.09, 0.60),
    ("IAS", "Iași", "Iași", "Moldova", 47.1585, 27.6014, 0.08, 0.70),
    ("CTA", "Constanța", "Constanța", "Dobrogea", 44.1598, 28.6348, 0.08, 0.55),
    ("BRV", "Brașov", "Brașov", "Transilvania", 45.6427, 25.5887, 0.07, 0.60),
    ("CRA", "Craiova", "Dolj", "Oltenia", 44.3302, 23.7949, 0.06, 0.50),
    ("SIB", "Sibiu", "Sibiu", "Transilvania", 45.7983, 24.1256, 0.05, 0.65),
    ("ORA", "Oradea", "Bihor", "Crișana", 47.0465, 21.9189, 0.05, 0.55),
    ("GAL", "Galați", "Galați", "Moldova", 45.4353, 28.0080, 0.05, 0.60),
]

PLATE_PREFIX = {
    "BUC": "B", "CLJ": "CJ", "TIM": "TM", "IAS": "IS", "CTA": "CT",
    "BRV": "BV", "CRA": "DJ", "SIB": "SB", "ORA": "BH", "GAL": "GL",
}

# Smaller towns served from each hub (delivered to the "PERIPHERAL" zone)
PERIPHERAL_TOWNS = {
    "BUC": ["Voluntari", "Otopeni", "Popești-Leordeni", "Buftea"],
    "CLJ": ["Florești", "Turda", "Dej", "Gherla"],
    "TIM": ["Dumbrăvița", "Lugoj", "Giroc", "Arad"],
    "IAS": ["Pașcani", "Vaslui", "Bacău", "Botoșani"],
    "CTA": ["Mamaia", "Medgidia", "Tulcea", "Năvodari"],
    "BRV": ["Săcele", "Sinaia", "Râșnov", "Predeal"],
    "CRA": ["Slatina", "Drobeta-Turnu Severin", "Târgu Jiu", "Caracal"],
    "SIB": ["Mediaș", "Alba Iulia", "Râmnicu Vâlcea", "Cisnădie"],
    "ORA": ["Salonta", "Beiuș", "Satu Mare", "Marghita"],
    "GAL": ["Brăila", "Focșani", "Tecuci", "Târgu Bujor"],
}

FIRST_NAMES = ["Andrei", "Mihai", "Ionuț", "Cristian", "Alexandru", "Florin", "Marius",
               "Gabriel", "Daniel", "Răzvan", "Bogdan", "Sorin", "Adrian", "Vasile",
               "Ovidiu", "Cătălin", "Nicolae", "Liviu", "Costel", "Dorin"]
LAST_NAMES = ["Popescu", "Ionescu", "Popa", "Dumitru", "Stan", "Gheorghe", "Constantin",
              "Marin", "Radu", "Stoica", "Munteanu", "Rusu", "Dinu", "Matei", "Barbu",
              "Toma", "Lungu", "Moldovan", "Neagu", "Preda"]

CUSTOMER_PREFIX = ["Nord", "Sud", "Carpat", "Delta", "Dunărea", "Orizont", "Meridian",
                   "Atlas", "Zenit", "Aurora", "Vega", "Lumina", "Tomis", "Ardeal",
                   "Banat", "Olt", "Mureș", "Prut", "Argeș", "Siret"]
SEGMENTS = {
    # segment: (share of customers, suffixes, sla_hours, p_express, p_cod, weight_mu)
    "ECOMMERCE": (0.55, ["Shop", "Market", "Fashion", "Home", "Tech"], 72, 0.10, 0.40, 0.6),
    "RETAIL": (0.15, ["Retail", "Mega", "Depot"], 72, 0.10, 0.05, 1.4),
    "PHARMA": (0.10, ["Pharma", "Farm", "Med"], 24, 0.60, 0.00, 0.2),
    "B2B": (0.20, ["Trade", "Industrial", "Supply", "Components"], 48, 0.20, 0.00, 1.8),
}

FAILURE_REASONS = ["RECIPIENT_ABSENT", "PHONE_UNREACHABLE", "WRONG_ADDRESS", "ACCESS_DENIED", "REFUSED_BY_RECIPIENT"]

WEEKDAY_FACTOR = [1.10, 1.05, 1.00, 1.00, 1.05, 0.55, 0.12]  # Mon..Sun

EVENT_COLUMNS = ["event_id", "shipment_id", "event_type", "event_ts",
                 "hub_id", "driver_id", "vehicle_id", "failure_reason"]
SHIPMENT_COLUMNS = ["shipment_id", "customer_id", "origin_hub_id", "destination_hub_id",
                    "destination_city", "destination_zone", "service_level", "weight_kg",
                    "declared_value_ron", "cod_amount_ron", "created_at", "promised_delivery_date"]


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def add_business_days(d: date, n: int) -> date:
    """Add n business days (Mon-Fri)."""
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def next_delivery_day(d: date) -> date:
    """Couriers deliver Mon-Sat; Sunday rolls over to Monday."""
    while d.weekday() == 6:
        d += timedelta(days=1)
    return d


def random_time_on(rng: random.Random, d: date, start_h: float, end_h: float) -> datetime:
    secs = rng.randint(int(start_h * 3600), int(end_h * 3600) - 1)
    return datetime.combine(d, time()) + timedelta(seconds=secs)


def black_friday(year: int) -> date:
    d = date(year, 11, 1)
    first_thursday = d + timedelta(days=(3 - d.weekday()) % 7)
    return first_thursday + timedelta(days=21 + 1)


def seasonal_factor(d: date) -> float:
    """Volume multiplier for peak seasons."""
    rel = (d - black_friday(d.year)).days
    bf_bump = {-3: 1.2, -2: 1.3, -1: 1.5, 0: 2.2, 1: 1.6, 2: 1.4, 3: 1.7, 4: 1.4, 5: 1.25, 6: 1.15}
    if rel in bf_bump:
        return bf_bump[rel]
    if d.month == 12:
        if d.day <= 20:
            return 1.35
        if d.day <= 24:
            return 1.50
        return 0.50
    if d.month == 1:
        return 0.90
    return 1.0


def overload_delay_days(rng: random.Random, load_ratio: float) -> int:
    """Extra days of delay caused by a congested hub (0, 1 or 2)."""
    p = min(max((load_ratio - 0.9) * 1.0, 0.0), 0.75)
    if rng.random() < p:
        return 1 if rng.random() < 0.75 else 2
    return 0


def weighted_choice(rng: random.Random, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


# --------------------------------------------------------------------------- #
# Dimension builders
# --------------------------------------------------------------------------- #
def build_hubs() -> list[dict]:
    """Hubs without capacity yet: capacity depends on the customers, see size_hub_capacity."""
    hubs = []
    for i, (code, city, county, region, lat, lon, share, util) in enumerate(HUB_SPECS, start=1):
        hubs.append({
            "hub_id": f"H{i:02d}", "hub_code": code, "city": city, "county": county,
            "region": region, "latitude": lat, "longitude": lon,
            "capacity_per_day": 0, "share": share, "util": util,
        })
    return hubs


def size_hub_capacity(hubs: list[dict], customers: list[dict], volume_weights: list[float], base_volume: int) -> None:
    """Set each hub's daily capacity from the volume that really STARTS there.

    A parcel starts at its customer's home hub (85 %) or at a random hub (15 %).
    A few big shippers dominate the volume, so a hub's real share of origin
    volume can be far from its nominal share. Sizing capacity from the nominal
    share would leave some hubs permanently over capacity.
    """
    total_weight = sum(volume_weights)
    total_share = sum(h["share"] for h in hubs)
    origin_share = {h["hub_id"]: 0.15 * h["share"] / total_share for h in hubs}
    for customer, weight in zip(customers, volume_weights):
        origin_share[customer["home_hub_id"]] += 0.85 * weight / total_weight
    for h in hubs:
        expected_per_day = base_volume * origin_share[h["hub_id"]]
        h["capacity_per_day"] = max(int(round(expected_per_day / h["util"], -1)), 10)


def build_fleet(rng: random.Random, hubs: list[dict]):
    vehicles, drivers = [], []
    vid, did = 1, 1
    for hub in hubs:
        prefix = PLATE_PREFIX[hub["hub_code"]]
        n_drivers = max(3, hub["capacity_per_day"] // 40)
        for _ in range(n_drivers):
            vehicles.append(_vehicle(rng, vid, prefix, hub["hub_id"], rng.choice(["VAN", "VAN", "ELECTRIC_VAN"])))
            vid += 1
            hire = date(2018, 1, 1) + timedelta(days=rng.randint(0, 2400))
            drivers.append({
                "driver_id": f"D{did:04d}",
                "full_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "hub_id": hub["hub_id"], "hire_date": hire.isoformat(),
                "license_category": "B", "is_active": "TRUE",
            })
            did += 1
        for _ in range(rng.randint(1, 2) + (1 if hub["capacity_per_day"] > 200 else 0)):
            vehicles.append(_vehicle(rng, vid, prefix, hub["hub_id"], "TRUCK"))
            vid += 1
        vehicles.append(_vehicle(rng, vid, prefix, hub["hub_id"], "VAN"))  # spare
        vid += 1
    return vehicles, drivers


def _vehicle(rng: random.Random, vid: int, prefix: str, hub_id: str, vtype: str) -> dict:
    letters = "".join(rng.choice("ABCDEFGHJKLMNPRSTUVXZ") for _ in range(3))
    number = rng.randint(100, 999) if prefix == "B" else rng.randint(10, 99)
    capacity = {"VAN": rng.choice([1000, 1200, 1500]), "ELECTRIC_VAN": 800,
                "TRUCK": rng.choice([8000, 10000, 12000])}[vtype]
    return {
        "vehicle_id": f"V{vid:04d}", "plate_number": f"{prefix} {number} {letters}",
        "vehicle_type": vtype, "capacity_kg": capacity, "hub_id": hub_id,
        "fuel_type": "ELECTRIC" if vtype == "ELECTRIC_VAN" else "DIESEL",
        "status": "MAINTENANCE" if rng.random() < 0.05 else "ACTIVE",
    }


def build_customers(rng: random.Random, hubs: list[dict], n: int):
    seg_names = list(SEGMENTS)
    seg_weights = [SEGMENTS[s][0] for s in seg_names]
    hub_weights = [h["share"] for h in hubs]
    customers, volume_weights, used = [], [], set()
    for i in range(1, n + 1):
        segment = weighted_choice(rng, seg_names, seg_weights)
        while True:
            name = f"{rng.choice(CUSTOMER_PREFIX)} {rng.choice(SEGMENTS[segment][1])} SRL"
            if name not in used:
                used.add(name)
                break
        home = weighted_choice(rng, hubs, hub_weights)
        customers.append({
            "customer_id": f"C{i:03d}", "customer_name": name, "segment": segment,
            "home_hub_id": home["hub_id"], "contract_sla_hours": SEGMENTS[segment][2],
            "onboarded_date": (date(2019, 1, 1) + timedelta(days=rng.randint(0, 2000))).isoformat(),
        })
        volume_weights.append(rng.paretovariate(1.4))  # a few big shippers, many small
    return customers, volume_weights


# --------------------------------------------------------------------------- #
# Shipment simulation
# --------------------------------------------------------------------------- #
class Simulator:
    def __init__(self, rng: random.Random, hubs, vehicles, drivers, customers, volume_weights, end_dt):
        self.rng = rng
        self.hubs = hubs
        self.hub_by_id = {h["hub_id"]: h for h in hubs}
        self.hub_weights = [h["share"] for h in hubs]
        self.customers = customers
        self.customer_by_id = {c["customer_id"]: c for c in customers}
        self.volume_weights = volume_weights
        self.end_dt = end_dt
        self.drivers_by_hub = defaultdict(list)
        for d in drivers:
            self.drivers_by_hub[d["hub_id"]].append(d["driver_id"])
        self.van_by_hub = defaultdict(list)
        self.truck_by_hub = defaultdict(list)
        for v in vehicles:
            if v["status"] != "ACTIVE":
                continue
            (self.truck_by_hub if v["vehicle_type"] == "TRUCK" else self.van_by_hub)[v["hub_id"]].append(v["vehicle_id"])
        self.dist = {
            (a["hub_id"], b["hub_id"]): haversine_km(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
            for a in hubs for b in hubs
        }
        self.shipment_seq = 0
        self.event_seq = 0

    # -- shipment attributes ------------------------------------------------ #
    def new_shipment(self, d: date, load_ratio: float) -> dict:
        rng = self.rng
        customer = weighted_choice(rng, self.customers, self.volume_weights)
        seg = SEGMENTS[customer["segment"]]
        origin = customer["home_hub_id"] if rng.random() < 0.85 else weighted_choice(rng, self.hubs, self.hub_weights)["hub_id"]
        dest = weighted_choice(rng, self.hubs, self.hub_weights)
        if rng.random() < 0.65:
            zone, city = "URBAN", dest["city"]
        else:
            zone, city = "PERIPHERAL", rng.choice(PERIPHERAL_TOWNS[dest["hub_code"]])
        service = "EXPRESS" if rng.random() < seg[3] else "STANDARD"
        weight = round(min(max(math.exp(rng.gauss(seg[5], 0.8)), 0.1), 30.0), 2)
        declared = round(rng.uniform(40, 400) + weight * rng.uniform(5, 60), 2)
        is_cod = rng.random() < seg[4]
        # most parcels are registered in office hours, the rest spread across the day
        created = random_time_on(rng, d, 9, 17) if rng.random() < 0.6 else random_time_on(rng, d, 7, 21)
        same_hub = origin == dest["hub_id"]
        days = {"STANDARD": 2 if same_hub else 3, "EXPRESS": 1 if same_hub else 2}[service]
        self.shipment_seq += 1
        return {
            "shipment_id": f"RO{1_000_000_000 + self.shipment_seq}",
            "customer_id": customer["customer_id"],
            "origin_hub_id": origin, "destination_hub_id": dest["hub_id"],
            "destination_city": city, "destination_zone": zone, "service_level": service,
            "weight_kg": f"{weight:.2f}", "declared_value_ron": f"{declared:.2f}",
            "cod_amount_ron": f"{declared:.2f}" if is_cod else "",
            "created_at": created, "promised_delivery_date": add_business_days(created.date(), days),
            "_is_cod": is_cod, "_load_ratio": load_ratio,
        }

    # -- event helpers ------------------------------------------------------- #
    def _event(self, sh, etype, ts, hub_id="", driver_id="", vehicle_id="", reason=""):
        self.event_seq += 1
        return (f"E{self.event_seq:010d}", sh["shipment_id"], etype, ts,
                hub_id, driver_id, vehicle_id, reason)

    def _driver_and_van(self, hub_id):
        drivers = self.drivers_by_hub[hub_id]
        vans = self.van_by_hub[hub_id]
        return self.rng.choice(drivers), self.rng.choice(vans)

    # -- the parcel journey --------------------------------------------------- #
    def simulate(self, sh) -> list[tuple]:
        rng = self.rng
        o, dst = sh["origin_hub_id"], sh["destination_hub_id"]
        created: datetime = sh["created_at"]
        ratio = sh["_load_ratio"]
        events = [self._event(sh, "CREATED", created, hub_id=o)]

        # pick-up
        if created.hour < 15 and created.weekday() != 6:
            pickup = created + timedelta(minutes=rng.randint(90, 300))
            if pickup.hour >= 20 or pickup.date() != created.date():
                pickup = random_time_on(rng, created.date(), 19, 19.9)
        else:
            pickup = random_time_on(rng, next_delivery_day(created.date() + timedelta(days=1)), 8.5, 12)
        driver, van = self._driver_and_van(o)
        events.append(self._event(sh, "PICKED_UP", pickup, o, driver, van))

        inbound_origin = pickup + timedelta(minutes=rng.randint(60, 180))
        events.append(self._event(sh, "HUB_INBOUND", inbound_origin, o))

        # linehaul between hubs (night run)
        if o != dst:
            depart_date = inbound_origin.date() if inbound_origin.hour < 18 else inbound_origin.date() + timedelta(days=1)
            depart = random_time_on(rng, depart_date, 19.5, 23.0)
            depart += timedelta(days=overload_delay_days(rng, ratio))
            disruption_p = 0.03 + (0.04 if created.month in (12, 1, 2) else 0.0)
            if rng.random() < disruption_p:
                depart += timedelta(days=1)
            truck = rng.choice(self.truck_by_hub[o]) if self.truck_by_hub[o] else ""
            events.append(self._event(sh, "HUB_OUTBOUND", depart, o, "", truck))
            travel_h = self.dist[(o, dst)] / 55.0 + 1.0 + rng.uniform(0, 1.5)
            arrival = depart + timedelta(hours=travel_h)
            events.append(self._event(sh, "HUB_INBOUND", arrival, dst))
            ready = arrival + timedelta(days=overload_delay_days(rng, ratio))
        else:
            ready = inbound_origin + timedelta(days=overload_delay_days(rng, ratio))

        # first delivery day
        threshold = 11 if sh["service_level"] == "EXPRESS" else 6
        ofd_date = ready.date() if ready.hour < threshold else ready.date() + timedelta(days=1)
        if sh["destination_zone"] == "PERIPHERAL" and rng.random() < 0.15:
            ofd_date += timedelta(days=1)
        ofd_date = next_delivery_day(ofd_date)

        # delivery attempts (max 3)
        for attempt in range(1, 4):
            driver, van = self._driver_and_van(dst)
            ofd_ts = random_time_on(rng, ofd_date, 7.5, 10)
            attempt_ts = ofd_ts + timedelta(minutes=rng.randint(60, 480))
            events.append(self._event(sh, "OUT_FOR_DELIVERY", ofd_ts, dst, driver, van))
            p_success = 0.94 if attempt == 1 else 0.85
            if sh["_is_cod"]:
                p_success -= 0.06
            if sh["destination_zone"] == "PERIPHERAL":
                p_success -= 0.05
            if rng.random() < p_success:
                events.append(self._event(sh, "DELIVERED", attempt_ts, dst, driver, van))
                break
            weights = [0.50, 0.15, 0.12, 0.05, 0.18 if sh["_is_cod"] else 0.04]
            reason = weighted_choice(rng, FAILURE_REASONS, weights)
            events.append(self._event(sh, "ATTEMPT_FAILED", attempt_ts, dst, driver, van, reason))
            if attempt == 3:
                ret_date = next_delivery_day(attempt_ts.date() + timedelta(days=rng.randint(1, 2)))
                events.append(self._event(sh, "RETURNED_TO_SENDER", random_time_on(rng, ret_date, 10, 16), dst))
                break
            ofd_date = next_delivery_day(ofd_date + timedelta(days=1))
        return events


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def day_volume(rng: random.Random, d: date, idx: int, base: int) -> int:
    v = base * WEEKDAY_FACTOR[d.weekday()] * seasonal_factor(d) * (1 + 0.0004 * idx) * rng.gauss(1, 0.06)
    return max(5, int(round(v)))


def write_csv(path: Path, columns: list[str], rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(columns)
        w.writerows(rows)


def generate(start: date, days: int, base_volume: int, seed: int, out: Path, n_customers: int = 60) -> dict:
    rng = random.Random(seed)
    hubs = build_hubs()
    customers, volume_weights = build_customers(rng, hubs, n_customers)
    size_hub_capacity(hubs, customers, volume_weights, base_volume)  # before the fleet: fleet size follows capacity
    vehicles, drivers = build_fleet(rng, hubs)

    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "hubs.csv", ["hub_id", "hub_code", "city", "county", "region", "latitude", "longitude", "capacity_per_day"],
              [[h[k] for k in ("hub_id", "hub_code", "city", "county", "region", "latitude", "longitude", "capacity_per_day")] for h in hubs])
    write_csv(out / "vehicles.csv", list(vehicles[0].keys()), [list(v.values()) for v in vehicles])
    write_csv(out / "drivers.csv", list(drivers[0].keys()), [list(d.values()) for d in drivers])
    write_csv(out / "customers.csv", list(customers[0].keys()), [list(c.values()) for c in customers])

    end_date = start + timedelta(days=days - 1)
    end_dt = datetime.combine(end_date, time(23, 59, 59))
    sim = Simulator(rng, hubs, vehicles, drivers, customers, volume_weights, end_dt)

    pending: dict[date, list[tuple]] = defaultdict(list)  # file date -> [(is_late, ts, row)]
    stats = {"shipments": 0, "events": 0, "duplicates": 0, "late": 0}

    def flush(upto: date | None) -> None:
        for fdate in sorted(k for k in pending if upto is None or k < upto):
            rows = sorted(pending.pop(fdate), key=lambda r: (r[0], r[1]))
            write_csv(out / "shipment_events" / f"shipment_events_{fdate.isoformat()}.csv",
                      EVENT_COLUMNS, [_fmt_event(r[2]) for r in rows])

    for idx in range(days):
        d = start + timedelta(days=idx)
        total = day_volume(rng, d, idx, base_volume)
        shipments = [sim.new_shipment(d, 0.0) for _ in range(total)]
        # load ratio = parcels that actually started at the hub today / its capacity.
        # This is the same number as MARTS.FCT_HUB_DAILY.CAPACITY_UTILISATION, so the
        # delays the simulation produces can be explained by the warehouse KPI.
        started_at = Counter(s["origin_hub_id"] for s in shipments)
        for sh in shipments:
            sh["_load_ratio"] = started_at[sh["origin_hub_id"]] / sim.hub_by_id[sh["origin_hub_id"]]["capacity_per_day"]
        shipments.sort(key=lambda s: s["created_at"])
        write_csv(out / "shipments" / f"shipments_{d.isoformat()}.csv", SHIPMENT_COLUMNS,
                  [[s["shipment_id"], s["customer_id"], s["origin_hub_id"], s["destination_hub_id"],
                    s["destination_city"], s["destination_zone"], s["service_level"], s["weight_kg"],
                    s["declared_value_ron"], s["cod_amount_ron"], s["created_at"].strftime(TS_FMT),
                    s["promised_delivery_date"].isoformat()] for s in shipments])
        stats["shipments"] += len(shipments)

        for sh in shipments:
            for ev in sim.simulate(sh):
                ts = ev[3]
                if ts > end_dt:
                    continue  # still in transit when the extract ends
                file_date = ts.date()
                is_late = 0
                if rng.random() < 0.003:
                    file_date = min(file_date + timedelta(days=rng.randint(1, 3)), end_date)
                    is_late = 1 if file_date != ts.date() else 0
                    stats["late"] += is_late
                pending[file_date].append((is_late, ts, ev))
                stats["events"] += 1
                if rng.random() < 0.004:
                    pending[file_date].append((is_late, ts, ev))
                    stats["duplicates"] += 1
        flush(upto=d)

    flush(upto=None)
    stats.update(start=start.isoformat(), end=end_date.isoformat(), hubs=len(hubs),
                 vehicles=len(vehicles), drivers=len(drivers), customers=len(customers))
    return stats


def _fmt_event(ev: tuple) -> list[str]:
    event_id, shipment_id, etype, ts, hub, driver, vehicle, reason = ev
    return [event_id, shipment_id, etype, ts.strftime(TS_FMT), hub, driver, vehicle, reason]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default="2025-10-01", help="first day of data (YYYY-MM-DD)")
    p.add_argument("--days", type=int, default=180, help="number of days to simulate")
    p.add_argument("--base-volume", type=int, default=600, help="average weekday parcels/day nationwide")
    p.add_argument("--customers", type=int, default=60)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/raw", help="output directory")
    a = p.parse_args()
    stats = generate(date.fromisoformat(a.start), a.days, a.base_volume, a.seed, Path(a.out), a.customers)
    print("Generated synthetic logistics data in", a.out)
    for k, v in stats.items():
        print(f"  {k:<11} {v}")


if __name__ == "__main__":
    main()
