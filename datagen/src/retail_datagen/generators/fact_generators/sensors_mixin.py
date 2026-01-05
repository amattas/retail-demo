"""
Sensor data generation (foot traffic and BLE pings)
"""
from __future__ import annotations

import logging
from datetime import datetime
import pandas as pd
from retail_datagen.shared.models import Store

logger = logging.getLogger(__name__)


class SensorsMixin:
    """Sensor data generation (foot traffic and BLE pings)"""

    def _generate_foot_traffic(
        self, store: Store, hour_datetime: datetime, receipt_count: int
    ) -> list[dict]:
        """Vectorized foot traffic generator using NumPy for per-sensor aggregates.

        Builds a compact DataFrame and converts to records to minimize
        Python per-row overhead. Falls back to loop-based generation if
        pandas is unavailable (logged at warning level).
        """
        # If no receipts, still may have some foot traffic (browsers)
        if receipt_count == 0:
            if self._rng.random() > 0.7:
                return []
            receipt_count = 1

        hour = hour_datetime.hour
        is_weekend = hour_datetime.weekday() >= 5

        base_conversion = 0.20
        if hour in [12, 13, 17, 18, 19]:
            conv_adj = 1.3
        elif hour in [10, 11, 14, 15, 16]:
            conv_adj = 1.0
        elif hour in [8, 9, 20, 21]:
            conv_adj = 0.7
        else:
            conv_adj = 0.5
        if is_weekend:
            conv_adj *= 0.9
        conversion_rate = base_conversion * conv_adj

        total_foot_traffic = max(
            receipt_count + 1, int(receipt_count / max(conversion_rate, 1e-6))
        )

        zones = np.array([
            "ENTRANCE_MAIN",
            "ENTRANCE_SIDE",
            "AISLES_A",
            "AISLES_B",
            "CHECKOUT",
        ])
        # Zone mix varies by store format for more realistic distributions
        fmt = getattr(store, 'store_format', 'standard') or 'standard'
        base_prop = {
            'hypermarket': np.array([0.20, 0.10, 0.35, 0.25, 0.10]),
            'superstore':  np.array([0.25, 0.10, 0.30, 0.25, 0.10]),
            'standard':    np.array([0.30, 0.15, 0.25, 0.15, 0.15]),
            'neighborhood':np.array([0.35, 0.15, 0.20, 0.10, 0.20]),
            'express':     np.array([0.45, 0.15, 0.10, 0.05, 0.25]),
        }
        proportions = base_prop.get(fmt, base_prop['standard']).astype(np.float64)
        base_counts = np.floor(total_foot_traffic * proportions).astype(np.int32)
        # Add variance by format (±10% to ±20%)
        var_mult = {'hypermarket': 0.20, 'superstore': 0.15, 'standard': 0.10, 'neighborhood': 0.12, 'express': 0.10}.get(fmt, 0.10)
        variance = np.floor(base_counts * var_mult).astype(np.int32)
        # Draw symmetric noise per zone
        noise = np.array([
            0 if v <= 0 else self._np_rng.integers(-v, v + 1)
            for v in variance
        ], dtype=np.int32)
        counts = np.maximum(0, base_counts + noise)

        # Dwell ranges per zone
        dwell_min = np.array([45, 30, 180, 120, 90], dtype=np.int32)
        dwell_max = np.array([90, 75, 420, 300, 240], dtype=np.int32)
        dwell = self._np_rng.integers(dwell_min, dwell_max + 1)

        try:
            import pandas as _pd

            df = _pd.DataFrame(
                {
                    "TraceId": [self._generate_trace_id()] * len(zones),
                    "EventTS": [hour_datetime] * len(zones),
                    "StoreID": [store.ID] * len(zones),
                    "SensorId": [f"SENSOR_{store.ID:03d}_{z}" for z in zones],
                    "Zone": zones.astype(str),
                    "Dwell": dwell.astype(int),
                    "Count": counts.astype(int),
                }
            )
            return df.to_dict("records")
        except Exception as e:
            # Fallback to simple list if pandas unavailable
            logger.warning(f"Failed to create BLE pings via pandas, using fallback: {e}")
            return [
                {
                    "TraceId": self._generate_trace_id(),
                    "EventTS": hour_datetime,
                    "StoreID": store.ID,
                    "SensorId": f"SENSOR_{store.ID:03d}_{zones[i]}",
                    "Zone": str(zones[i]),
                    "Dwell": int(dwell[i]),
                    "Count": int(counts[i]),
                }
                for i in range(len(zones))
            ]


    def _generate_ble_pings(
        self, store: Store, customer: Customer, transaction_time: datetime
    ) -> list[dict]:
        """Generate BLE beacon pings with store-format variability and anonymity.

        - 30% known devices (BLEId linked to customer, include customer_id)
        - 70% anonymous devices (random BLE IDs not present in customer master)
        - Pings per visit scale by store format and traffic multiplier

        Falls back to loop-based generation if pandas DataFrame operations
        fail (logged at warning level for operational monitoring).
        """
        zones_all = np.array(["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"], dtype=object)
        beacons_all = np.array([f"BEACON_{store.ID:03d}_{z}" for z in zones_all], dtype=object)
        # Choose 2-4 distinct zones with format-specific weights
        fmt = getattr(store, 'store_format', 'standard') or 'standard'
        zone_weights_map = {
            'hypermarket': np.array([0.20, 0.25, 0.30, 0.15, 0.10]),
            'superstore':  np.array([0.25, 0.20, 0.30, 0.15, 0.10]),
            'standard':    np.array([0.30, 0.15, 0.25, 0.15, 0.15]),
            'neighborhood':np.array([0.35, 0.10, 0.25, 0.10, 0.20]),
            'express':     np.array([0.40, 0.05, 0.20, 0.05, 0.30]),
        }
        w = zone_weights_map.get(fmt, zone_weights_map['standard'])
        k = int(self._np_rng.integers(2, 5))
        pick_idx = np.array(self._np_rng.choice(len(zones_all), size=k, replace=False, p=(w / w.sum())))

        # Base per-zone pings and multiplier by format and traffic
        per_zone_counts = self._np_rng.integers(2, 6, size=k)  # 2-5 pings per chosen zone
        fmt_mult_map = {'hypermarket': 1.6, 'superstore': 1.3, 'standard': 1.0, 'neighborhood': 0.85, 'express': 0.65}
        traffic_mult = float(getattr(store, 'daily_traffic_multiplier', Decimal("1.0")))
        ble_mult = fmt_mult_map.get(fmt, 1.0) * max(0.6, min(1.8, traffic_mult))
        per_zone_counts = np.maximum(1, (per_zone_counts.astype(float) * ble_mult)).astype(np.int32)

        total = int(per_zone_counts.sum())
        if total <= 0:
            return []
        zones_rep = np.repeat(zones_all[pick_idx], per_zone_counts)
        beacons_rep = np.repeat(beacons_all[pick_idx], per_zone_counts)
        rssi = self._np_rng.integers(-80, -29, size=total)
        offsets = self._np_rng.integers(-15, 16, size=total)

        # Vectorized record assembly
        try:
            import pandas as _pd

            # Known vs anonymous device mask
            known_mask = self._np_rng.random(total) < 0.30
            # Anonymous BLE IDs
            anon_suffix = self._np_rng.integers(100000, 1000000, size=total)
            anon_ids = np.array(
                [f"ANON-{store.ID}-{int(x)}" for x in anon_suffix], dtype=object
            )
            ble_ids = np.where(known_mask, customer.BLEId, anon_ids)
            cust_ids = np.where(known_mask, customer.ID, None)

            event_ts_list = [
                transaction_time + timedelta(minutes=int(m)) for m in offsets
            ]

            df = _pd.DataFrame(
                {
                    "TraceId": [self._generate_trace_id() for _ in range(total)],
                    "EventTS": event_ts_list,
                    "StoreID": np.repeat(store.ID, total),
                    "BeaconId": beacons_rep.astype(object),
                    "CustomerBLEId": ble_ids.astype(object),
                    "CustomerId": cust_ids,
                    "RSSI": rssi.astype(int),
                    "Zone": zones_rep.astype(object),
                }
            )
            return df.to_dict("records")
        except Exception as e:
            # Fallback loop if pandas unavailable
            logger.warning(f"Failed to create foot traffic via pandas, using fallback: {e}")
            out: list[dict] = []
            for i in range(total):
                is_known = self._rng.random() < 0.30
                if is_known:
                    ble_id = customer.BLEId
                    cust_id = customer.ID
                else:
                    ble_id = f"ANON-{store.ID}-{self._rng.randint(100000, 999999)}"
                    cust_id = None
                out.append(
                    {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": transaction_time + timedelta(minutes=int(offsets[i])),
                        "StoreID": store.ID,
                        "BeaconId": str(beacons_rep[i]),
                        "CustomerBLEId": ble_id,
                        "CustomerId": cust_id,
                        "RSSI": int(rssi[i]),
                        "Zone": str(zones_rep[i]),
                    }
                )
            return out


