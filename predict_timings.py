import sqlite3
import json
from pathlib import Path
from datetime import datetime
from statistics import mean
import pandas as pd

def predict_stop_times_segmental(
    data_input,
    db_path="db/live_data.db",
    output_path="generated_in/times.json",
    bin_minutes=15,
    min_samples_route=3,
    min_samples_global=5,
    bin_window=2,       # search ±N bins around target
    target_date=None    # "YYYY-MM-DD" -> lock DoW; None -> use DoW-aware fallbacks
):
    """
    Trip-start-agnostic, segment-level predictor with DoW + time-of-day bins.
    Fallback order per segment (A->B):
      1) route+DoW+bin (±bin_window), then global+DoW+bin
      2) route any-day, then global any-day
      3) CHAIN via X: (A->X at t) + (X->B at t+ΔAX), best candidate
      4) A-><any> (outbound mean) using route/global DoW+bin ±window, then any-day
      5) <any>->B (inbound mean) using route/global DoW+bin ±window, then any-day
      6) 0 (unknown)
    Writes JSON to output_path with stop_time (HHMM int) and confidence (sample count).
    """

    # ---------- Helpers ----------
    def time_to_minutes(hhmm):
        h, m = map(int, hhmm.split(":"))
        return h * 60 + m

    def minutes_to_hhmm_int(minutes):
        minutes %= 1440
        h, m = divmod(minutes, 60)
        return int(f"{h:02d}{m:02d}")

    def weekday_from_str(datestr):
        return datetime.strptime(datestr, "%Y-%m-%d").weekday()

    def unwrap_trip_minutes(times_min):
        unwrapped, offset, prev = [], 0, None
        for t in times_min:
            if prev is not None and t + offset < prev:
                offset += 1440
            val = t + offset
            unwrapped.append(val)
            prev = val
        return unwrapped

    def time_bin_idx(mins):
        return (mins % 1440) // bin_minutes

    target_dow = weekday_from_str(target_date) if target_date else None

    # ---------- Load DB ----------
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        "SELECT stop_id, trip_id, route_id, date, actual_arrival, actual_departure "
        "FROM completed_stop_times",
        conn
    )
    conn.close()

    # Choose departure if available, else arrival
    time_col = df["actual_departure"].where(
        df["actual_departure"].notna() & (df["actual_departure"] != ""),
        df["actual_arrival"]
    )
    df = df.assign(use_time=time_col)
    df = df[df["use_time"].notna() & (df["use_time"] != "")]
    df["stop_id"] = df["stop_id"].astype(str)
    df["route_id"] = df["route_id"].astype(str)
    df["dow"] = df["date"].apply(weekday_from_str)
    df["mins"] = df["use_time"].apply(time_to_minutes)

    # ---------- Build segments ----------
    segments = []
    for trip_id, g in df.groupby("trip_id"):
        g = g.sort_values("mins")
        mins_unwrapped = unwrap_trip_minutes(g["mins"].tolist())
        stops = g["stop_id"].tolist()
        routes = g["route_id"].tolist()
        dows = g["dow"].tolist()
        for i in range(len(stops) - 1):
            a, b = stops[i], stops[i + 1]
            r = routes[i]
            dow = dows[i]
            ta, tb = mins_unwrapped[i], mins_unwrapped[i + 1]
            delta = tb - ta
            if delta < 0:
                continue
            bin_id = time_bin_idx(ta)
            segments.append((r, a, b, dow, bin_id, delta))

    if not segments:
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump([], f, indent=2)
        return []

    seg_df = pd.DataFrame(segments, columns=["route_id", "stop_a", "stop_b", "dow", "bin", "delta"])

    # ---------- Stats tables ----------
    def agg_stats(df, keys):
        return df.groupby(keys)["delta"].agg(["count", "mean"]).reset_index()

    # Direct A->B
    route_stats = agg_stats(seg_df, ["route_id", "stop_a", "stop_b", "dow", "bin"])
    global_stats = agg_stats(seg_df, ["stop_a", "stop_b", "dow", "bin"])
    route_stats_anyday = agg_stats(seg_df, ["route_id", "stop_a", "stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )
    global_stats_anyday = agg_stats(seg_df, ["stop_a", "stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )

    # A-><any> outbound means
    route_out = agg_stats(seg_df, ["route_id", "stop_a", "dow", "bin"])
    global_out = agg_stats(seg_df, ["stop_a", "dow", "bin"])
    route_out_anyday = agg_stats(seg_df, ["route_id", "stop_a"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )
    global_out_anyday = agg_stats(seg_df, ["stop_a"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )

    # <any>->B inbound means
    route_in = agg_stats(seg_df, ["route_id", "stop_b", "dow", "bin"])
    global_in = agg_stats(seg_df, ["stop_b", "dow", "bin"])
    route_in_anyday = agg_stats(seg_df, ["route_id", "stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )
    global_in_anyday = agg_stats(seg_df, ["stop_b"]).rename(
        columns={"mean": "mean_anyday", "count": "count_anyday"}
    )

    # Maps for quick lookup
    route_map = { (r.route_id, r.stop_a, r.stop_b, int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
                  for _, r in route_stats.iterrows() }
    global_map = { (r.stop_a, r.stop_b, int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
                   for _, r in global_stats.iterrows() }
    route_anyday_map = { (r.route_id, r.stop_a, r.stop_b): (int(r["count_anyday"]), float(r["mean_anyday"]))
                         for _, r in route_stats_anyday.iterrows() }
    global_anyday_map = { (r.stop_a, r.stop_b): (int(r["count_anyday"]), float(r["mean_anyday"]))
                          for _, r in global_stats_anyday.iterrows() }

    route_out_map = { (r.route_id, r.stop_a, int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
                      for _, r in route_out.iterrows() }
    global_out_map = { (r.stop_a, int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
                       for _, r in global_out.iterrows() }
    route_out_anyday_map = { (r.route_id, r.stop_a): (int(r["count_anyday"]), float(r["mean_anyday"]))
                             for _, r in route_out_anyday.iterrows() }
    global_out_anyday_map = { (r.stop_a,): (int(r["count_anyday"]), float(r["mean_anyday"]))
                              for _, r in global_out_anyday.iterrows() }

    route_in_map = { (r.route_id, r.stop_b, int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
                     for _, r in route_in.iterrows() }
    global_in_map = { (r.stop_b, int(r.dow), int(r["bin"])): (int(r["count"]), float(r["mean"]))
                      for _, r in global_in.iterrows() }
    route_in_anyday_map = { (r.route_id, r.stop_b): (int(r["count_anyday"]), float(r["mean_anyday"]))
                            for _, r in route_in_anyday.iterrows() }
    global_in_anyday_map = { (r.stop_b,): (int(r["count_anyday"]), float(r["mean_anyday"]))
                             for _, r in global_in_anyday.iterrows() }

    # Adjacency sets for chaining
    succ_from_A = seg_df.groupby("stop_a")["stop_b"].apply(set).to_dict()
    pred_to_B = seg_df.groupby("stop_b")["stop_a"].apply(set).to_dict()

    n_bins_total = 1440 // bin_minutes

    def search_bins(map_dict, key_base, bin_center, bin_limit):
        for d in range(-bin_limit, bin_limit + 1):
            b = (bin_center + d) % n_bins_total
            k = key_base + (b,)
            if k in map_dict:
                return map_dict[k]
        return None

    # ---------- Direct stat lookup with fallbacks ----------
    def stat_direct(route_id, a, b, time_at_a_mins, dow_pref):
        """Return (mean, count) using direct A->B with DoW+bin±window, else any-day."""
        bin_center = time_bin_idx(time_at_a_mins)
        # route + DoW + bin±window
        if dow_pref is not None:
            val = search_bins(route_map, (route_id, a, b, dow_pref), bin_center, bin_window)
            if val and val[0] >= min_samples_route:
                return val[1], val[0]
        # global + DoW + bin±window
        if dow_pref is not None:
            val = search_bins(global_map, (a, b, dow_pref), bin_center, bin_window)
            if val and val[0] >= min_samples_global:
                return val[1], val[0]
        # route any-day
        kr = (route_id, a, b)
        if kr in route_anyday_map and route_anyday_map[kr][0] >= min_samples_route:
            c, m = route_anyday_map[kr]
            return m, c
        # global any-day
        kg = (a, b)
        if kg in global_anyday_map and global_anyday_map[kg][0] >= min_samples_global:
            c, m = global_anyday_map[kg]
            return m, c
        return None, 0

    def stat_outbound(route_id, a, time_at_a_mins, dow_pref):
        """A-><any> mean (use as estimate for A->B when direct missing)."""
        bin_center = time_bin_idx(time_at_a_mins)
        if dow_pref is not None:
            val = search_bins(route_out_map, (route_id, a, dow_pref), bin_center, bin_window)
            if val and val[0] >= min_samples_route:
                return val[1], val[0]
            val = search_bins(global_out_map, (a, dow_pref), bin_center, bin_window)
            if val and val[0] >= min_samples_global:
                return val[1], val[0]
        kr = (route_id, a)
        if kr in route_out_anyday_map and route_out_anyday_map[kr][0] >= min_samples_route:
            c, m = route_out_anyday_map[kr]
            return m, c
        kg = (a,)
        if kg in global_out_anyday_map and global_out_anyday_map[kg][0] >= min_samples_global:
            c, m = global_out_anyday_map[kg]
            return m, c
        return None, 0

    def stat_inbound(route_id, b, time_at_a_mins, dow_pref):
        """<any>->B mean (use as estimate for A->B when direct/outbound missing)."""
        bin_center = time_bin_idx(time_at_a_mins)
        if dow_pref is not None:
            val = search_bins(route_in_map, (route_id, b, dow_pref), bin_center, bin_window)
            if val and val[0] >= min_samples_route:
                return val[1], val[0]
            val = search_bins(global_in_map, (b, dow_pref), bin_center, bin_window)
            if val and val[0] >= min_samples_global:
                return val[1], val[0]
        kr = (route_id, b)
        if kr in route_in_anyday_map and route_in_anyday_map[kr][0] >= min_samples_route:
            c, m = route_in_anyday_map[kr]
            return m, c
        kg = (b,)
        if kg in global_in_anyday_map and global_in_anyday_map[kg][0] >= min_samples_global:
            c, m = global_in_anyday_map[kg]
            return m, c
        return None, 0

    # ---------- Chaining via X ----------
    def stat_chain(route_id, a, b, time_at_a_mins, dow_pref):
        """
        Try A->X at t, then X->B at t+ΔAX for X in successors(A) ∩ predecessors(B).
        Score by higher min(count1, count2), break ties by lower total mean.
        """
        succ = succ_from_A.get(a, set())
        pred = pred_to_B.get(b, set())
        candidates = list(succ & pred)
        best = (None, 0, None)  # (total_mean, combined_conf, chosen_X)

        for x in candidates:
            m1, c1 = stat_direct(route_id, a, x, time_at_a_mins, dow_pref)
            if m1 is None:
                continue
            t_at_x = time_at_a_mins + int(round(m1))
            m2, c2 = stat_direct(route_id, x, b, t_at_x, dow_pref)
            if m2 is None:
                continue
            total_mean = m1 + m2
            conf = min(c1, c2)  # conservative confidence
            if best[1] < conf or (best[1] == conf and (best[0] is None or total_mean < best[0])):
                best = (total_mean, conf, x)

        if best[0] is not None:
            return best[0], best[1]
        return None, 0

    # ---------- Unified predictor for a segment ----------
    def predict_segment(route_id, a, b, time_at_a_mins, dow_pref):
        # 1) Direct A->B
        m, c = stat_direct(route_id, a, b, time_at_a_mins, dow_pref)
        if m is not None:
            return m, c
        # 2) Chain via X
        m, c = stat_chain(route_id, a, b, time_at_a_mins, dow_pref)
        if m is not None:
            return m, c
        # 3) A-><any>
        mA, cA = stat_outbound(route_id, a, time_at_a_mins, dow_pref)
        # 4) <any>->B
        mB, cB = stat_inbound(route_id, b, time_at_a_mins, dow_pref)
        if mA is not None and mB is not None:
            # If both exist, take the smaller (more conservative forward step) and sum confidences
            if mA <= mB:
                return mA, cA
            else:
                return mB, cB
        if mA is not None:
            return mA, cA
        if mB is not None:
            return mB, cB
        # 5) Unknown
        return 0, 0

    # ---------- Build output (cumulative chaining along provided stops) ----------
    results = []
    for trip in data_input:
        route_id_str = str(trip.get("route_id", ""))
        trip_start_str = trip["trip_start"]
        base_mins = time_to_minutes(trip_start_str)
        dow_for_trip = target_dow
        stops_in = trip.get("stops", [])
        if not stops_in:
            results.append({**trip, "stops": []})
            continue

        cum_mins = base_mins
        out_stops = []
        # First stop = trip_start
        first = stops_in[0]
        out_stops.append({
            "stop_id": first["stop_id"],
            "stop_loc": first.get("stop_loc"),
            "stop_time": minutes_to_hhmm_int(cum_mins),
            "confidence": None
        })

        for i in range(len(stops_in) - 1):
            a = str(stops_in[i]["stop_id"])
            b = str(stops_in[i + 1]["stop_id"])
            seg_mean, conf = predict_segment(route_id_str, a, b, cum_mins, dow_for_trip)
            cum_mins += int(round(seg_mean))
            out_stops.append({
                "stop_id": stops_in[i + 1]["stop_id"],
                "stop_loc": stops_in[i + 1].get("stop_loc"),
                "stop_time": minutes_to_hhmm_int(cum_mins),
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
