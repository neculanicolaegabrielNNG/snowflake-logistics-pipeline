"""Sanity tests for the synthetic data generator (standard library only).

Run from the repository root:
    python3 -m unittest discover -s tests -v
"""
import csv
import hashlib
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data_generator"))
import generate_data as gen  # noqa: E402

TS = "%Y-%m-%d %H:%M:%S"
VALID_EVENT_TYPES = {"CREATED", "PICKED_UP", "HUB_INBOUND", "HUB_OUTBOUND", "OUT_FOR_DELIVERY",
                     "ATTEMPT_FAILED", "DELIVERED", "RETURNED_TO_SENDER"}


def read_all(folder: Path):
    rows = []
    for f in sorted(folder.glob("*.csv")):
        with f.open(encoding="utf-8", newline="") as fh:
            rows.extend(csv.DictReader(fh))
    return rows


def read_one(path: Path):
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def folder_hash(root: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(root.rglob("*.csv")):
        h.update(f.relative_to(root).as_posix().encode())
        h.update(f.read_bytes())
    return h.hexdigest()


class GeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name) / "raw"
        cls.stats = gen.generate(date(2025, 11, 17), 21, 150, 123, cls.out, n_customers=15)
        cls.hubs = read_one(cls.out / "hubs.csv")
        cls.customers = read_one(cls.out / "customers.csv")
        cls.drivers = read_one(cls.out / "drivers.csv")
        cls.vehicles = read_one(cls.out / "vehicles.csv")
        cls.shipments = read_all(cls.out / "shipments")
        cls.events = read_all(cls.out / "shipment_events")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_row_counts_match_stats(self):
        self.assertEqual(len(self.shipments), self.stats["shipments"])
        self.assertEqual(len(self.events), self.stats["events"] + self.stats["duplicates"])
        self.assertEqual(len(self.hubs), 10)

    def test_primary_keys_are_unique(self):
        self.assertEqual(len({s["shipment_id"] for s in self.shipments}), len(self.shipments))
        self.assertEqual(len({h["hub_id"] for h in self.hubs}), len(self.hubs))
        self.assertEqual(len({d["driver_id"] for d in self.drivers}), len(self.drivers))

    def test_duplicate_events_are_exact_copies(self):
        by_id = {}
        for e in self.events:
            if e["event_id"] in by_id:
                self.assertEqual(by_id[e["event_id"]], e)
            by_id[e["event_id"]] = e
        self.assertEqual(len(self.events) - len(by_id), self.stats["duplicates"])

    def test_foreign_keys(self):
        hub_ids = {h["hub_id"] for h in self.hubs}
        cust_ids = {c["customer_id"] for c in self.customers}
        shipment_ids = {s["shipment_id"] for s in self.shipments}
        driver_ids = {d["driver_id"] for d in self.drivers}
        vehicle_ids = {v["vehicle_id"] for v in self.vehicles}
        for s in self.shipments:
            self.assertIn(s["customer_id"], cust_ids)
            self.assertIn(s["origin_hub_id"], hub_ids)
            self.assertIn(s["destination_hub_id"], hub_ids)
        for e in self.events:
            self.assertIn(e["shipment_id"], shipment_ids)
            self.assertIn(e["event_type"], VALID_EVENT_TYPES)
            if e["hub_id"]:
                self.assertIn(e["hub_id"], hub_ids)
            if e["driver_id"]:
                self.assertIn(e["driver_id"], driver_ids)
            if e["vehicle_id"]:
                self.assertIn(e["vehicle_id"], vehicle_ids)

    def test_timeline_is_consistent(self):
        created = {s["shipment_id"]: datetime.strptime(s["created_at"], TS) for s in self.shipments}
        for e in self.events:
            self.assertGreaterEqual(datetime.strptime(e["event_ts"], TS), created[e["shipment_id"]])

    def test_failure_reason_only_on_failed_attempts(self):
        for e in self.events:
            self.assertEqual(bool(e["failure_reason"]), e["event_type"] == "ATTEMPT_FAILED")

    def test_events_do_not_exceed_extract_end(self):
        for e in self.events:
            self.assertLessEqual(datetime.strptime(e["event_ts"], TS).date(), date(2025, 12, 7))

    def test_parcel_ends_in_at_most_one_final_state(self):
        finals = {}
        for e in {e["event_id"]: e for e in self.events}.values():
            if e["event_type"] in ("DELIVERED", "RETURNED_TO_SENDER"):
                finals.setdefault(e["shipment_id"], []).append(e["event_type"])
        for sid, types in finals.items():
            self.assertEqual(len(types), 1, sid)

    def test_cod_amount_equals_declared_value(self):
        for s in self.shipments:
            if s["cod_amount_ron"]:
                self.assertEqual(s["cod_amount_ron"], s["declared_value_ron"])

    def test_weight_is_positive(self):
        for s in self.shipments:
            self.assertGreater(float(s["weight_kg"]), 0)

    def test_generation_is_deterministic(self):
        with tempfile.TemporaryDirectory() as other:
            gen.generate(date(2025, 11, 17), 21, 150, 123, Path(other), n_customers=15)
            self.assertEqual(folder_hash(self.out), folder_hash(Path(other)))

    def test_no_hub_is_permanently_over_capacity(self):
        # Regression: capacity used to be sized from a nominal share, so hubs that host the big
        # shippers ran at 200-400 % of capacity every day. Average utilisation must stay below 100 %.
        capacity = {h["hub_id"]: int(h["capacity_per_day"]) for h in self.hubs}
        started = {}
        for s in self.shipments:
            key = (s["origin_hub_id"], s["created_at"][:10])
            started[key] = started.get(key, 0) + 1
        days = {s["created_at"][:10] for s in self.shipments}
        for hub_id, cap in capacity.items():
            avg_utilisation = sum(started.get((hub_id, d), 0) for d in days) / len(days) / cap
            self.assertLess(avg_utilisation, 1.0, hub_id)

    def test_black_friday_date(self):
        self.assertEqual(gen.black_friday(2025), date(2025, 11, 28))
        self.assertEqual(gen.black_friday(2026), date(2026, 11, 27))


if __name__ == "__main__":
    unittest.main()
