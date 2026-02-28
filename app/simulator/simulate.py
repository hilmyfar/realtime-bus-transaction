"""
simulator/simulate.py
=====================:

ASUMSI:
- 10 halte (H-A paling utara → H-J paling selatan)
- Jarak antar halte: 5 menit
- A→J (full trip): 50 menit (9 segmen × 5 menit)
- Turnaround di ujung: 5 menit
- 1 siklus penuh (A→J→A): 50 + 5 + 50 + 5 = 110 menit
- Tiap bus, masing-masing offset keberangkatan agar tidak bersamaan
- Window: 06:00–18:00

ALUR:
1. Generate jadwal bus → kapan tiap bus tiba di tiap halte sepanjang hari
2. Assign penumpang ke trip tertentu (bus + keberangkatan tertentu)
3. tap_in_time  = waktu bus tiba di halte naik  ± jitter ≤30 detik
4. tap_out_time = waktu bus tiba di halte turun ± jitter ≤30 detik
5. Koordinat    = koordinat halte ± noise kecil (~10m)
"""

import uuid
import random
import time
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Tuple, Dict, Generator

# ============================================================
# KONFIGURASI
# ============================================================
NUM_CARDS           = 2000
NUM_BUSES           = 7
SIM_DATE            = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
START_HOUR          = 6
END_HOUR            = 12

MINUTES_PER_SEGMENT = 5     # waktu tempuh antar halte
TURNAROUND_MINUTES  = 5     # waktu balik di ujung (H-A atau H-J)
JITTER_SECONDS      = 30    # toleransi waktu tap ±30 detik dari kedatangan bus
COORD_NOISE_M       = 10    # noise koordinat dalam meter

# Offset keberangkatan tiap bus dari jam START_HOUR
# Bus-01 berangkat 06:00, Bus-02 06:15, Bus-03 06:30
BUS_OFFSETS_MIN = {
    "BUS-01": 0,
    "BUS-02": 15,
    "BUS-03": 30,
    "BUS-04": 45,
    "BUS-05": 60,
    "BUS-06": 75,
    "BUS-07": 90,
}

# 10 halte urut utara → selatan
HALTE: List[Tuple[str, str, float, float]] = [
    ("H-A", "Halte Kota",            -6.1375, 106.8135),
    ("H-B", "Halte Mangga Besar",    -6.1482, 106.8178),
    ("H-C", "Halte Sawah Besar",     -6.1589, 106.8221),
    ("H-D", "Halte Juanda",          -6.1696, 106.8264),
    ("H-E", "Halte Monas",           -6.1750, 106.8272),
    ("H-F", "Halte Bank Indonesia",  -6.1804, 106.8280),
    ("H-G", "Halte Bundaran HI",     -6.1934, 106.8230),
    ("H-H", "Halte Dukuh Atas",      -6.2012, 106.8230),
    ("H-I", "Halte Bendungan Hilir", -6.2134, 106.8198),
    ("H-J", "Halte Semanggi",        -6.2256, 106.8162),
]

HALTE_IDS = [h[0] for h in HALTE]
CARD_IDS  = [f"C{str(i).zfill(3)}" for i in range(1, NUM_CARDS + 1)]
BUS_IDS   = list(BUS_OFFSETS_MIN.keys())
N_HALTE   = len(HALTE)

# 1 siklus penuh A→J→A dalam menit
CYCLE_MIN = (N_HALTE - 1) * MINUTES_PER_SEGMENT * 2 + TURNAROUND_MINUTES * 2
# = 9*5*2 + 5*2 = 90 + 10 = 100 menit


# ============================================================
# DATA CLASS
# ============================================================
@dataclass
class BusTransaction:
    transaction_id: str
    card_id:        str
    bus_id:         str
    event_type:     str   # "tap_in" | "tap_out"
    timestamp:      int   # Unix ms
    latitude:       float
    longitude:      float

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class BusStop:
    """Representasi kedatangan bus di satu halte."""
    bus_id:    str
    halte_idx: int
    halte_id:  str
    arrival:   datetime
    direction: str   # "forward" (A→J) | "backward" (J→A)


# ============================================================
# HELPER
# ============================================================
def jitter_coord(lat: float, lon: float, radius_m: float = COORD_NOISE_M) -> Tuple[float, float]:
    """Noise kecil pada koordinat untuk mensimulasikan GPS bus."""
    delta_lat = random.uniform(-radius_m, radius_m) / 111_000
    delta_lon = random.uniform(-radius_m, radius_m) / 111_000
    return round(lat + delta_lat, 6), round(lon + delta_lon, 6)


def jitter_time(dt: datetime, max_seconds: int = JITTER_SECONDS) -> datetime:
    """Variasi kecil pada waktu tap — penumpang tidak selalu tap tepat saat bus tiba."""
    offset = random.randint(0, max_seconds)
    return dt + timedelta(seconds=offset)


# ============================================================
# 1. GENERATE JADWAL BUS
# Hasilkan kapan tiap bus tiba di tiap halte sepanjang hari
# ============================================================
def generate_bus_schedule() -> Dict[str, List[BusStop]]:
    schedule: Dict[str, List[BusStop]] = {bus_id: [] for bus_id in BUS_IDS}
    window_end = SIM_DATE + timedelta(hours=END_HOUR)

    for bus_id, offset_min in BUS_OFFSETS_MIN.items():
        current_time = SIM_DATE + timedelta(hours=START_HOUR, minutes=offset_min)
        direction    = "forward"  # mulai A→J

        while current_time < window_end:
            halte_range = range(N_HALTE) if direction == "forward" else range(N_HALTE - 1, -1, -1)

            for idx in halte_range:
                if current_time >= window_end:
                    break
                h_id = HALTE[idx][0]
                schedule[bus_id].append(BusStop(
                    bus_id    = bus_id,
                    halte_idx = idx,
                    halte_id  = h_id,
                    arrival   = current_time,
                    direction = direction,
                ))
                current_time += timedelta(minutes=MINUTES_PER_SEGMENT)

            # Turnaround di ujung
            current_time += timedelta(minutes=TURNAROUND_MINUTES)
            direction = "backward" if direction == "forward" else "forward"

    return schedule


# ============================================================
# 2. KELOMPOKKAN JADWAL MENJADI TRIP SEGMENTS
# 1 trip = 1 perjalanan satu arah (A→J atau J→A)
# ============================================================
def get_trip_segments(schedule: Dict[str, List[BusStop]]) -> List[Dict]:
    trips = []

    for bus_id, stops in schedule.items():
        if not stops:
            continue

        current_dir   = stops[0].direction
        current_group = [stops[0]]

        for stop in stops[1:]:
            if stop.direction != current_dir:
                if len(current_group) >= 2:
                    trips.append({
                        "bus_id":    bus_id,
                        "direction": current_dir,
                        "stops":     current_group,
                    })
                current_group = [stop]
                current_dir   = stop.direction
            else:
                current_group.append(stop)

        if len(current_group) >= 2:
            trips.append({
                "bus_id":    bus_id,
                "direction": current_dir,
                "stops":     current_group,
            })

    return trips


# ============================================================
# 3. GENERATE SEMUA TRANSAKSI PENUMPANG
# ============================================================
def generate_all_transactions(max_trips_per_card: int = 2) -> List[BusTransaction]:
    schedule      = generate_bus_schedule()
    trip_segments = get_trip_segments(schedule)

    if not trip_segments:
        raise RuntimeError("Tidak ada trip yang ter-generate.")

    all_events: List[BusTransaction] = []

    for card_id in CARD_IDS:
        n_trips           = random.randint(1, max_trips_per_card)
        last_tap_out_time = datetime.min
        trips_done        = 0
        attempts          = 0

        while trips_done < n_trips and attempts < n_trips * 10:
            attempts += 1

            trip  = random.choice(trip_segments)
            stops = trip["stops"]

            if len(stops) < 2:
                continue

            board_idx  = random.randint(0, len(stops) - 2)
            alight_idx = random.randint(board_idx + 1, len(stops) - 1)

            stop_in  = stops[board_idx]
            stop_out = stops[alight_idx]

            tap_in_time  = jitter_time(stop_in.arrival)
            tap_out_time = jitter_time(stop_out.arrival)

            if tap_out_time <= tap_in_time:
                tap_out_time = stop_out.arrival + timedelta(minutes=MINUTES_PER_SEGMENT)

            # Skip jika durasi > 50 menit (max A→J atau J→A)
            duration_min = (tap_out_time - tap_in_time).total_seconds() / 60
            if duration_min > 50:
                continue

            # Skip jika overlap dengan trip sebelumnya (beri jeda 5 menit)
            if tap_in_time <= last_tap_out_time + timedelta(minutes=5):
                continue

            _, _, lat_in,  lon_in  = HALTE[stop_in.halte_idx]
            _, _, lat_out, lon_out = HALTE[stop_out.halte_idx]
            jlat_in,  jlon_in  = jitter_coord(lat_in,  lon_in)
            jlat_out, jlon_out = jitter_coord(lat_out, lon_out)

            all_events.append(BusTransaction(
                transaction_id = str(uuid.uuid4()),
                card_id        = card_id,
                bus_id         = trip["bus_id"],
                event_type     = "tap_in",
                timestamp      = int(tap_in_time.timestamp() * 1000),
                latitude       = jlat_in,
                longitude      = jlon_in,
            ))
            all_events.append(BusTransaction(
                transaction_id = str(uuid.uuid4()),
                card_id        = card_id,
                bus_id         = trip["bus_id"],
                event_type     = "tap_out",
                timestamp      = int(tap_out_time.timestamp() * 1000),
                latitude       = jlat_out,
                longitude      = jlon_out,
            ))

            last_tap_out_time = tap_out_time
            trips_done       += 1

    all_events.sort(key=lambda e: e.timestamp)
    return all_events


# ============================================================
# STREAM GENERATOR (untuk producer Kafka)
# ============================================================
def stream_transactions(
    interval_ms: int = 500,
    max_trips_per_card: int = 3
) -> Generator[BusTransaction, None, None]:
    transactions = generate_all_transactions(max_trips_per_card=max_trips_per_card)
    print(f"[SIMULATOR] Total events : {len(transactions)}")
    print(f"[SIMULATOR] Interval     : {interval_ms}ms per event")
    for event in transactions:
        yield event
        time.sleep(interval_ms / 1000.0)


# ============================================================
# MAIN: Dry run — preview jadwal + sample transaksi
# ============================================================
if __name__ == "__main__":
    print("=" * 70)
    print("BUS SMART CARD SIMULATOR — DRY RUN")
    print("=" * 70)

    schedule = generate_bus_schedule()

    print(f"\nKONFIGURASI:")
    print(f"  Halte             : {N_HALTE} (H-A s/d H-J)")
    print(f"  Menit per segmen  : {MINUTES_PER_SEGMENT} menit")
    print(f"  Turnaround        : {TURNAROUND_MINUTES} menit")
    print(f"  Durasi A→J        : {(N_HALTE-1)*MINUTES_PER_SEGMENT} menit")
    print(f"  1 siklus A→J→A    : {CYCLE_MIN} menit")
    print(f"  Window            : {START_HOUR:02d}:00 – {END_HOUR:02d}:00 ")
    print(f"  Siklus/bus        : ~{360 // CYCLE_MIN} siklus")

    print(f"\nJADWAL BUS (21 stop pertama per bus):")
    print("-" * 70)
    for bus_id, stops in schedule.items():
        print(f"\n  {bus_id} (offset +{BUS_OFFSETS_MIN[bus_id]} menit):")
        for stop in stops[:21]:
            arah = "→ selatan" if stop.direction == "forward" else "→ utara"
            print(f"    {stop.arrival.strftime('%H:%M')}  {stop.halte_id}  {arah}")
        print(f"    ... total {len(stops)} kedatangan sepanjang hari")

    trip_segs = get_trip_segments(schedule)
    print(f"\nTRIP SEGMENTS (satu arah):")
    by_bus = {}
    for t in trip_segs:
        by_bus.setdefault(t["bus_id"], {"forward": 0, "backward": 0})
        by_bus[t["bus_id"]][t["direction"]] += 1
    for bus_id, counts in by_bus.items():
        total = counts["forward"] + counts["backward"]
        print(f"  {bus_id}: {total} trip  "
              f"(A→J: {counts['forward']}, J→A: {counts['backward']})")

    print(f"\nGENERATE TRANSAKSI PENUMPANG...")
    transactions = generate_all_transactions(max_trips_per_card=3)
    tap_in_count  = sum(1 for t in transactions if t.event_type == "tap_in")
    tap_out_count = sum(1 for t in transactions if t.event_type == "tap_out")

    print(f"\nRINGKASAN TRANSAKSI:")
    print(f"  Total events   : {len(transactions)}")
    print(f"  tap_in         : {tap_in_count}")
    print(f"  tap_out        : {tap_out_count}")
    print(f"  Total penumpang: {NUM_CARDS}")

    print(f"\nSAMPLE 10 EVENT PERTAMA:")
    print("-" * 70)
    for tx in transactions[:10]:
        dt = tx.timestamp
        print(f"  [{dt}]  {tx.card_id}  {tx.bus_id}  "
              f"{tx.event_type:<8}  ({tx.latitude:.4f}, {tx.longitude:.4f})")
    print(f"\nSave result:")
    with open("sample_transactions.json", "w") as f:
        json.dump([t.to_dict() for t in transactions], f, indent=2)
    print(f"  → sample_transactions.json")