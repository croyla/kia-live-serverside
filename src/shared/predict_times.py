#!/usr/bin/env python3
"""
predict_times.py

Single-file module that provides:
  - build_segment_stats(db_path, bin_minutes)
  - predict_stop_times_segmental(...): full-trip prediction (fills all stop times)
  - predict_stop_times_partial(...): partial prediction (preserves pre-filled times and fills the gaps)

Both methods are:
  * Trip-start agnostic (use time-of-day bins keyed by the time at stop A)
  * Day-of-week aware (prefer same DoW, fallback to any-day)
  * Route-specific but route-agnostic fallbacks included
  * Include ±bin neighbor windowing
  * Support chaining A->X->B when A->B is unseen
  * Additional A-><any> and <any>->B fallbacks
  * Return confidence as sample counts backing each predicted segment

JSON output shape:
[
  {
    "route_id": <int|str>,
    "trip_start": "HH:MM",
    "stops": [
      {"stop_id": <id>, "stop_loc": [lng, lat], "stop_time": HHMM_int, "confidence": <int|null> },
      ...
    ]
  }
]
"""

from __future__ import annotations

import os, pickle, hashlib
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd


# ------------------------------
# Shared helpers
# ------------------------------

def _unwrap_trip_minutes(times_min: List[int]) -> List[int]:
    """Unwrap times that may cross midnight so sequence is nondecreasing in minutes."""
    unwrapped, offset, prev = [], 0, None
    for t in times_min:
        if prev is not None and t + offset < prev:
            offset += 1440
        val = t + offset
        unwrapped.append(val)
        prev = val
    return unwrapped


def _minutes_to_hhmm_int(minutes: int) -> int:
    """Convert minutes since midnight (may exceed 1440) to HHMM integer, wrapping at 24h."""
    minutes %= 1440
    h, m = divmod(minutes, 60)
    return int(f"{h:02d}{m:02d}")


def _hhmm_to_minutes(x: Any) -> Optional[int]:
    """Accept HHMM int (e.g., 2350) or 'HH:MM' string and return minutes since midnight."""
    if x is None:
        return None
    if isinstance(x, int):
        s = f"{x:04d}"
        return (int(s[:2]) * 60 + int(s[2:])) % 1440
    if isinstance(x, str) and ":" in x:
        h, m = map(int, x.split(":"))
        return (h * 60 + m) % 1440
    return None


def _weekday_from_str(datestr: str) -> int:
    """Monday=0 .. Sunday=6."""
    return datetime.strptime(datestr, "%Y-%m-%d").weekday()


# ------------------------------
# Stats builder
# ------------------------------

CACHE_DIR = os.environ.get("PREDICT_TIMES_CACHE", "generated_in/.cache")

_STATS_MEMO = {}  # {(db_path, mtime, bin_minutes): stats_dict}

def _db_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return -1.0

def _cache_key(db_path: str, bin_minutes: int) -> str:
    payload = json.dumps({
        "db": os.path.abspath(db_path),
        "mtime": _db_mtime(db_path),
        "bin": bin_minutes,
        "ver": 1  # bump if you change stats structure
    }, sort_keys=True)
    return hashlib.sha1(payload.encode()).hexdigest()

def _cache_path(db_path: str, bin_minutes: int) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"stats_{_cache_key(db_path, bin_minutes)}.pkl")

def build_segment_stats(db_path: str, bin_minutes: int = 15) -> Dict[str, Any]:
    """
    Build segment-level statistics from db_path:
      - Direct A->B means & counts by (route_id?, DoW, time-of-day bin at A)
      - Outbound A-><any> & inbound <any>->B means & counts
      - Any-day fallbacks (route-specific and global)
      - Adjacency sets for chaining

    Returns a dict with maps and settings.
    """
    # try in-memory first
    key = (os.path.abspath(db_path), _db_mtime(db_path), bin_minutes)
    if key in _STATS_MEMO:
        return _STATS_MEMO[key]

    # then on-disk
    cpath = _cache_path(db_path, bin_minutes)
    if os.path.exists(cpath):
        with open(cpath, "rb") as fh:
            stats = pickle.load(fh)
        _STATS_MEMO[key] = stats
        return stats

    def to_min(s: str) -> int:
        h, m = map(int, s.split(":"))
        return h * 60 + m

    # Load DB
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        "SELECT stop_id, trip_id, route_id, date, actual_arrival, actual_departure "
        "FROM completed_stop_times",
        conn
    )
    conn.close()

    # Canonical time: prefer departure else arrival
    time_col = df["actual_departure"].where(
        df["actual_departure"].notna() & (df["actual_departure"] != ""),
        df["actual_arrival"]
    )
    df = df.assign(use_time=time_col)
    df = df[df["use_time"].notna() & (df["use_time"] != "")].copy()

    # Normalize
    df["stop_id"] = df["stop_id"].astype(str)
    df["route_id"] = df["route_id"].astype(str)
    df["dow"] = df["date"].apply(_weekday_from_str)
    df["mins"] = df["use_time"].apply(to_min)

    # Build segments (A->B)
    seg_records: List[Tuple[str, str, str, int, int, int]] = []
    for trip_id, g in df.groupby("trip_id"):
        g = g.sort_values("mins")
        mins_un = _unwrap_trip_minutes(g["mins"].tolist())
        stops = g["stop_id"].tolist()
        routes = g["route_id"].tolist()
        dows = g["dow"].tolist()
        for i in range(len(stops) - 1):
            a, b = stops[i], stops[i + 1]
            r, dow = routes[i], dows[i]
            ta, tb = mins_un[i], mins_un[i + 1]
            delta = tb - ta
            if delta < 0:
                continue
            bin_id = (ta % 1440) // bin_minutes
            seg_records.append((r, a, b, dow, bin_id, delta))

    seg_df = pd.DataFrame(seg_records, columns=["route_id", "stop_a", "stop_b", "dow", "bin", "delta"])

    # Aggregations
    def agg_stats(dfin: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
        return dfin.groupby(keys)["delta"].agg(["count", "mean"]).reset_index()

    # Direct A->B
    route_stats = agg_stats(seg_df, ["route_id", "stop_a", "stop_b", "dow", "bin"])
    global_stats = agg_stats(seg_df, ["stop_a", "stop_b", "dow", "bin"])
    route_stats_anyday = agg_stats(seg_df, ["route_id", "stop_a", "stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )
    global_stats_anyday = agg_stats(seg_df, ["stop_a", "stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )

    # Outbound and inbound
    route_out = agg_stats(seg_df, ["route_id", "stop_a", "dow", "bin"])
    global_out = agg_stats(seg_df, ["stop_a", "dow", "bin"])
    route_out_anyday = agg_stats(seg_df, ["route_id", "stop_a"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )
    global_out_anyday = agg_stats(seg_df, ["stop_a"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )

    route_in = agg_stats(seg_df, ["route_id", "stop_b", "dow", "bin"])
    global_in = agg_stats(seg_df, ["stop_b", "dow", "bin"])
    route_in_anyday = agg_stats(seg_df, ["route_id", "stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )
    global_in_anyday = agg_stats(seg_df, ["stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )

    # Maps for fast lookup
    route_map = {
        (str(r.route_id), str(r.stop_a), str(r.stop_b), int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
        for _, r in route_stats.iterrows()
    }
    global_map = {
        (str(r.stop_a), str(r.stop_b), int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
        for _, r in global_stats.iterrows()
    }
    route_anyday_map = {
        (str(r.route_id), str(r.stop_a), str(r.stop_b)): (int(r["count_anyday"]), float(r["mean_anyday"]))
        for _, r in route_stats_anyday.iterrows()
    }
    global_anyday_map = {
        (str(r.stop_a), str(r.stop_b)): (int(r["count_anyday"]), float(r["mean_anyday"]))
        for _, r in global_stats_anyday.iterrows()
    }

    route_out_map = {
        (str(r.route_id), str(r.stop_a), int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
        for _, r in route_out.iterrows()
    }
    global_out_map = {
        (str(r.stop_a), int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
        for _, r in global_out.iterrows()
    }
    route_out_anyday_map = {
        (str(r.route_id), str(r.stop_a)): (int(r["count_anyday"]), float(r["mean_anyday"]))
        for _, r in route_out_anyday.iterrows()
    }
    global_out_anyday_map = {
        (str(r.stop_a),): (int(r["count_anyday"]), float(r["mean_anyday"]))
        for _, r in global_out_anyday.iterrows()
    }

    route_in_map = {
        (str(r.route_id), str(r.stop_b), int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
        for _, r in route_in.iterrows()
    }
    global_in_map = {
        (str(r.stop_b), int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
        for _, r in global_in.iterrows()
    }
    route_in_anyday_map = {
        (str(r.route_id), str(r.stop_b)): (int(r["count_anyday"]), float(r["mean_anyday"]))
        for _, r in route_in_anyday.iterrows()
    }
    global_in_anyday_map = {
        (str(r.stop_b),): (int(r["count_anyday"]), float(r["mean_anyday"]))
        for _, r in global_in_anyday.iterrows()
    }

    # Adjacency sets for chaining
    succ_from_A = seg_df.groupby("stop_a")["stop_b"].apply(set).to_dict()
    pred_to_B = seg_df.groupby("stop_b")["stop_a"].apply(set).to_dict()

    return {
        "bin_minutes": bin_minutes,
        "n_bins_total": 1440 // bin_minutes,
        "route_map": route_map,
        "global_map": global_map,
        "route_anyday_map": route_anyday_map,
        "global_anyday_map": global_anyday_map,
        "route_out_map": route_out_map,
        "global_out_map": global_out_map,
        "route_out_anyday_map": route_out_anyday_map,
        "global_out_anyday_map": global_out_anyday_map,
        "route_in_map": route_in_map,
        "global_in_map": global_in_map,
        "route_in_anyday_map": route_in_anyday_map,
        "global_in_anyday_map": global_in_anyday_map,
        "succ_from_A": succ_from_A,
        "pred_to_B": pred_to_B,
    }


# ------------------------------
# Core prediction primitives (shared)
# ------------------------------

def _search_bins(map_dict: Dict, key_base: Tuple, bin_center: int, bin_limit: int, n_bins_total: int):
    """Search ±bin_limit around bin_center for available stats."""
    for d in range(-bin_limit, bin_limit + 1):
        b = (bin_center + d) % n_bins_total
        k = key_base + (b,)
        if k in map_dict:
            return map_dict[k]
    return None


def _stat_direct(stats: Dict[str, Any], route_id: str, a: str, b: str, tA_mins: int, dow: Optional[int],
                 min_samples_route: int, min_samples_global: int, bin_window: int) -> Tuple[Optional[float], int]:
    """Direct A->B using DoW+bin±window; fallback to any-day; returns (mean, count)."""
    bin_center = (tA_mins % 1440) // stats["bin_minutes"]
    n_bins_total = stats["n_bins_total"]
    if dow is not None:
        val = _search_bins(stats["route_map"], (route_id, a, b, dow), bin_center, bin_window, n_bins_total)
        if val and val[0] >= min_samples_route:
            return val[1], val[0]
        val = _search_bins(stats["global_map"], (a, b, dow), bin_center, bin_window, n_bins_total)
        if val and val[0] >= min_samples_global:
            return val[1], val[0]
    kr = (route_id, a, b)
    if kr in stats["route_anyday_map"] and stats["route_anyday_map"][kr][0] >= min_samples_route:
        c, m = stats["route_anyday_map"][kr]
        return m, c
    kg = (a, b)
    if kg in stats["global_anyday_map"] and stats["global_anyday_map"][kg][0] >= min_samples_global:
        c, m = stats["global_anyday_map"][kg]
        return m, c
    return None, 0


def _stat_outbound(stats: Dict[str, Any], route_id: str, a: str, tA_mins: int, dow: Optional[int],
                   min_samples_route: int, min_samples_global: int, bin_window: int) -> Tuple[Optional[float], int]:
    """A-><any> mean as proxy; returns (mean, count)."""
    bin_center = (tA_mins % 1440) // stats["bin_minutes"]
    n_bins_total = stats["n_bins_total"]
    if dow is not None:
        val = _search_bins(stats["route_out_map"], (route_id, a, dow), bin_center, bin_window, n_bins_total)
        if val and val[0] >= min_samples_route:
            return val[1], val[0]
        val = _search_bins(stats["global_out_map"], (a, dow), bin_center, bin_window, n_bins_total)
        if val and val[0] >= min_samples_global:
            return val[1], val[0]
    kr = (route_id, a)
    if kr in stats["route_out_anyday_map"] and stats["route_out_anyday_map"][kr][0] >= min_samples_route:
        c, m = stats["route_out_anyday_map"][kr]
        return m, c
    kg = (a,)
    if kg in stats["global_out_anyday_map"] and stats["global_out_anyday_map"][kg][0] >= min_samples_global:
        c, m = stats["global_out_anyday_map"][kg]
        return m, c
    return None, 0


def _stat_inbound(stats: Dict[str, Any], route_id: str, b: str, tA_mins: int, dow: Optional[int],
                  min_samples_route: int, min_samples_global: int, bin_window: int) -> Tuple[Optional[float], int]:
    """<any>->B mean as proxy; returns (mean, count)."""
    bin_center = (tA_mins % 1440) // stats["bin_minutes"]
    n_bins_total = stats["n_bins_total"]
    if dow is not None:
        val = _search_bins(stats["route_in_map"], (route_id, b, dow), bin_center, bin_window, n_bins_total)
        if val and val[0] >= min_samples_route:
            return val[1], val[0]
        val = _search_bins(stats["global_in_map"], (b, dow), bin_center, bin_window, n_bins_total)
        if val and val[0] >= min_samples_global:
            return val[1], val[0]
    kr = (route_id, b)
    if kr in stats["route_in_anyday_map"] and stats["route_in_anyday_map"][kr][0] >= min_samples_route:
        c, m = stats["route_in_anyday_map"][kr]
        return m, c
    kg = (b,)
    if kg in stats["global_in_anyday_map"] and stats["global_in_anyday_map"][kg][0] >= min_samples_global:
        c, m = stats["global_in_anyday_map"][kg]
        return m, c
    return None, 0


def _stat_chain(stats: Dict[str, Any], route_id: str, a: str, b: str, tA_mins: int, dow: Optional[int],
                min_samples_route: int, min_samples_global: int, bin_window: int) -> Tuple[Optional[float], int]:
    """
    Chain via X: (A->X at tA) + (X->B at tA+ΔAX). Pick the candidate maximizing min(counts),
    then by smaller total mean.
    """
    succ = stats["succ_from_A"].get(a, set())
    pred = stats["pred_to_B"].get(b, set())
    candidates = list(succ & pred)
    best_total, best_conf = None, 0

    for x in candidates:
        m1, c1 = _stat_direct(stats, route_id, a, x, tA_mins, dow, min_samples_route, min_samples_global, bin_window)
        if m1 is None:
            continue
        tX = tA_mins + int(round(m1))
        m2, c2 = _stat_direct(stats, route_id, x, b, tX, dow, min_samples_route, min_samples_global, bin_window)
        if m2 is None:
            continue
        total = m1 + m2
        conf = min(c1, c2)
        if conf > best_conf or (conf == best_conf and (best_total is None or total < best_total)):
            best_total, best_conf = total, conf

    return (best_total, best_conf) if best_total is not None else (None, 0)


def _predict_segment(stats: Dict[str, Any], route_id: str, a: str, b: str, tA_mins: int, dow: Optional[int],
                     min_samples_route: int, min_samples_global: int, bin_window: int) -> Tuple[float, int]:
    """
    Unified predictor for one segment (A->B).
    Order: direct -> chain -> A->any / any->B -> unknown(0).
    Returns (duration_minutes, confidence_count).
    """
    m, c = _stat_direct(stats, route_id, a, b, tA_mins, dow, min_samples_route, min_samples_global, bin_window)
    if m is not None:
        return m, c
    m, c = _stat_chain(stats, route_id, a, b, tA_mins, dow, min_samples_route, min_samples_global, bin_window)
    if m is not None:
        return m, c
    mA, cA = _stat_outbound(stats, route_id, a, tA_mins, dow, min_samples_route, min_samples_global, bin_window)
    mB, cB = _stat_inbound(stats, route_id, b, tA_mins, dow, min_samples_route, min_samples_global, bin_window)
    if mA is not None and mB is not None:
        return (mA, cA) if mA <= mB else (mB, cB)
    if mA is not None:
        return mA, cA
    if mB is not None:
        return mB, cB
    return 0.0, 0


# ------------------------------
# Full-trip prediction
# ------------------------------

def predict_stop_times_segmental(
    data_input: List[Dict[str, Any]],
    db_path: str = "db/live_data.db",
    output_path: Union[str, None] = "generated_in/times.json",
    bin_minutes: int = 15,
    min_samples_route: int = 3,
    min_samples_global: int = 5,
    bin_window: int = 2,
    target_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Compute stop times for each trip in data_input using segment-level stats.
    Fills all stop times starting from trip_start. Outputs confidence per stop (min sample count).
    """
    stats = build_segment_stats(db_path, bin_minutes=bin_minutes)
    dow_pref = None if not target_date else _weekday_from_str(target_date)

    results: List[Dict[str, Any]] = []
    for trip in data_input:
        route_id = str(trip.get("route_id", ""))
        stops_in = trip.get("stops", [])
        if not stops_in:
            results.append({**trip, "stops": []})
            continue

        base_mins = _hhmm_to_minutes(trip["trip_start"])
        if base_mins is None:
            raise ValueError("trip_start must be provided as 'HH:MM' for full prediction.")

        cum_mins = base_mins
        out_stops: List[Dict[str, Any]] = []
        # First stop time is the start time
        first = stops_in[0]
        out_stops.append({
            "stop_id": first["stop_id"],
            "stop_loc": first.get("stop_loc"),
            "stop_time": _minutes_to_hhmm_int(cum_mins),
            "confidence": None
        })

        for i in range(len(stops_in) - 1):
            a = str(stops_in[i]["stop_id"])
            b = str(stops_in[i + 1]["stop_id"])
            dur, conf = _predict_segment(stats, route_id, a, b, cum_mins, dow_pref,
                                         min_samples_route, min_samples_global, bin_window)
            cum_mins += int(round(dur))
            out_stops.append({
                "stop_id": stops_in[i + 1]["stop_id"],
                "stop_loc": stops_in[i + 1].get("stop_loc"),
                "stop_time": _minutes_to_hhmm_int(cum_mins),
                "confidence": int(conf)
            })

        results.append({
            "route_id": trip.get("route_id"),
            "trip_start": trip["trip_start"],
            "stops": out_stops
        })
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)

    return results


# ------------------------------
# Partial prediction (gap filling)
# ------------------------------

def predict_stop_times_partial(
    data_input: List[Dict[str, Any]],
    db_path: str = "db/live_data.db",
    output_path: Union[str, None] = "generated_in/times.json",
    bin_minutes: int = 15,
    min_samples_route: int = 3,
    min_samples_global: int = 5,
    bin_window: int = 2,
    target_date: Optional[str] = None,
    enforce_end_anchors: bool = False
) -> List[Dict[str, Any]]:
    """
    Fills only missing stop_time values, preserving pre-filled stop_time values (anchors).
    stop_time may be HHMM int or 'HH:MM' string. Also uses trip['trip_start'] as an anchor for the first stop if present.
    """
    stats = build_segment_stats(db_path, bin_minutes=bin_minutes)
    dow_pref = None if not target_date else _weekday_from_str(target_date)

    results: List[Dict[str, Any]] = []

    def backward_time_from_b(route_id: str, a: str, b: str, time_b_mins: int) -> int:
        """
        Estimate time at A given the time at B with a tiny fixed-point iteration.
        """
        # seed using any-day means if available
        seed = 0
        kr = (route_id, a, b)
        if kr in stats["route_anyday_map"]:
            seed = stats["route_anyday_map"][kr][1]
        elif (a, b) in stats["global_anyday_map"]:
            seed = stats["global_anyday_map"][(a, b)][1]
        tA = time_b_mins - int(round(seed))
        for _ in range(2):
            dur, _ = _predict_segment(stats, route_id, a, b, tA, dow_pref, min_samples_route, min_samples_global, bin_window)
            tA = time_b_mins - int(round(dur))
        return tA

    for trip in data_input:
        route_id = str(trip.get("route_id", ""))
        stops = trip.get("stops", [])
        n = len(stops)
        if n == 0:
            results.append({**trip, "stops": []})
            continue

        times: List[Optional[int]] = [None] * n
        is_anchor: List[bool] = [False] * n
        confid: List[Optional[int]] = [None] * n

        # Use trip_start as anchor if provided
        base_start = _hhmm_to_minutes(trip.get("trip_start"))
        if base_start is not None:
            times[0] = base_start
            is_anchor[0] = True

        # Respect any provided stop_time in stops
        for i, s in enumerate(stops):
            st = s.get("stop_time")
            if st is not None:
                times[i] = _hhmm_to_minutes(st)
                is_anchor[i] = True

        # Forward pass: from each anchor, fill rightwards
        for i in range(n - 1):
            if times[i] is None:
                continue
            t = times[i]
            for j in range(i, n - 1):
                if times[j + 1] is not None:
                    break
                a = str(stops[j]["stop_id"])
                b = str(stops[j + 1]["stop_id"])
                dur, c = _predict_segment(stats, route_id, a, b, t, dow_pref,
                                          min_samples_route, min_samples_global, bin_window)
                t = (t + int(round(dur))) % 1440
                times[j + 1] = t
                confid[j + 1] = c if confid[j + 1] is None else max(confid[j + 1], c)

        # Backward pass: from each anchor, fill leftwards
        for i in range(n - 1, 0, -1):
            if times[i] is None:
                continue
            tB = times[i]
            for j in range(i - 1, -1, -1):
                if times[j] is not None:
                    break
                a = str(stops[j]["stop_id"])
                b = str(stops[j + 1]["stop_id"])
                tA = backward_time_from_b(route_id, a, b, tB)
                times[j] = tA % 1440
                # reuse direct prediction confidence for the forward step j->j+1
                dur, c = _predict_segment(stats, route_id, a, b, tA, dow_pref,
                                          min_samples_route, min_samples_global, bin_window)
                confid[j + 1] = c if confid[j + 1] is None else max(confid[j + 1], c)
                tB = tA

        # Optional: ensure gaps with both ends anchored align exactly to the right anchor
        if enforce_end_anchors:
            k = 0
            while k < n:
                if times[k] is None:
                    k += 1
                    continue
                # Find next anchored stop to the right
                r = k + 1
                while r < n and not is_anchor[r]:
                    r += 1
                if r < n and is_anchor[r]:
                    # Recompute proportionally between anchors
                    left, right = k, r
                    predicted_span = (times[right] - times[left]) % 1440
                    target_right = times[right]  # already anchored (either given or computed earlier)
                    if predicted_span != 0:
                        scale = ((target_right - times[left]) % 1440) / predicted_span
                        t = times[left]
                        for idx in range(left, right):
                            a = str(stops[idx]["stop_id"])
                            b = str(stops[idx + 1]["stop_id"])
                            base_dur, _ = _predict_segment(stats, route_id, a, b, t, dow_pref,
                                                            min_samples_route, min_samples_global, bin_window)
                            t = (t + int(round(base_dur * scale))) % 1440
                            times[idx + 1] = t
                k = r + 1

        # Build output
        out_stops: List[Dict[str, Any]] = []
        for i, s in enumerate(stops):
            out_stops.append({
                "stop_id": s["stop_id"],
                "stop_loc": s.get("stop_loc"),
                "stop_time": _minutes_to_hhmm_int(times[i]) if times[i] is not None else None,
                "confidence": confid[i] if confid[i] is not None else (None if is_anchor[i] else 0)
            })

        results.append({
            "route_id": trip.get("route_id"),
            "trip_start": trip.get("trip_start"),
            "stops": out_stops
        })
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)

    return results


# ------------------------------
# Optional CLI
# ------------------------------

def _load_json(path: str) -> Any:
    with open(path, "r") as f:
        return json.load(f)


def _save_json(path: str, obj: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Predict stop times from live history")
    p.add_argument("--db", default="db/live_data.db", help="Path to SQLite DB")
    p.add_argument("--infile", help="Input JSON path (array of trips)")
    p.add_argument("--outfile", default="generated_in/times.json", help="Output JSON path")
    p.add_argument("--bin", type=int, default=15, help="Time-of-day bin size in minutes")
    p.add_argument("--route-min", type=int, default=3, help="Min samples for route-specific stats")
    p.add_argument("--global-min", type=int, default=5, help="Min samples for global stats")
    p.add_argument("--window", type=int, default=2, help="±bin window search radius")
    p.add_argument("--target-date", default=None, help="YYYY-MM-DD to lock DoW; default None")
    sub = p.add_subparsers(dest="mode")
    sub.add_parser("full", help="Full-trip prediction (fills all stop times)")
    pp = sub.add_parser("partial", help="Partial prediction (preserve pre-filled stop_time and fill gaps)")
    pp.add_argument("--enforce-end-anchors", action="store_true", help="Force each gap to end exactly at right anchor")

    args = p.parse_args()

    trips = _load_json(args.infile) if args.infile else []
    if args.mode == "partial":
        res = predict_stop_times_partial(
            trips,
            db_path=args.db,
            output_path=args.outfile,
            bin_minutes=args.bin,
            min_samples_route=args.route_min,
            min_samples_global=args.global_min,
            bin_window=args.window,
            target_date=args.target_date,
            enforce_end_anchors=getattr(args, "enforce_end_anchors", False)
        )
    else:
        # default to full
        res = predict_stop_times_segmental(
            trips,
            db_path=args.db,
            output_path=args.outfile,
            bin_minutes=args.bin,
            min_samples_route=args.route_min,
            min_samples_global=args.global_min,
            bin_window=args.window,
            target_date=args.target_date
        )

    _save_json(args.outfile, res)
    print(f"Wrote {args.outfile}")
