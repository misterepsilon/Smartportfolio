import numpy as np
import pandas as pd
import params


# Trading-day approximations for momentum lookbacks: ~21 days/month and
# ~252 days/year. Calendar months/years don't align with trading sessions,
# so equity-factor literature uses these conventions.
_DAYS_PER_MONTH = 21
_DAYS_PER_6M = 126
_DAYS_PER_YEAR = 252


def _passthrough(permnos, metric_keys, reason, selected=1):
    """Build a default-shape result dict where every permno shares one reason.

    Used when a filter cannot evaluate the cross-section (e.g. date not in
    index, history too short). `selected=1` is the permissive default —
    skip-conditions should not silently drop names — but callers pass
    `selected=0` when the universe itself is unusable (e.g. no permno is
    present in the data).
    """
    base = {"selected": selected, "reason": reason}
    base.update({k: np.nan for k in metric_keys})
    return {p: dict(base) for p in permnos}


def liquidity_filter(prices, volumes, permnos, decision_date,
                     min_volume_usd, min_price, max_price,
                     lookback_days, min_volume_coverage=0.7):
    """Filter permnos by recent average dollar volume and price band.

    Uses the price at `t-1` as the current reference (no look-ahead) and
    the mean USD volume over `[t-lookback_days, t)`. Permnos with stale
    volume coverage, prices outside `[min_price, max_price]`, or volume
    below the threshold are rejected with a per-asset reason.

    Args:
        prices: DataFrame of prices, index=dates, columns=permnos.
        volumes: DataFrame of USD volumes, same shape as `prices`.
        permnos: Permnos to evaluate.
        decision_date: Decision date `t`. Price reference is `t-1`.
        min_volume_usd: Minimum acceptable mean USD volume.
        min_price: Minimum acceptable price at `t-1`.
        max_price: Maximum acceptable price at `t-1`.
        lookback_days: Volume-window length.
        min_volume_coverage: Minimum fraction of non-NaN observations
            required in the volume window.

    Returns:
        dict mapping permno -> {selected, current_price, avg_volume_usd, reason}.

    Raises:
        Any error other than `KeyError` on `decision_date` propagates —
        only a missing date triggers a passthrough.
    """
    metric_keys = ("current_price", "avg_volume_usd")

    try:
        t = prices.index.get_loc(decision_date)
    except KeyError:
        return _passthrough(permnos, metric_keys, "date_not_in_index")

    if t < lookback_days:
        return _passthrough(permnos, metric_keys, "insufficient_history_passthrough")

    cols_prices = [p for p in permnos if p in prices.columns]
    cols_vols = [p for p in permnos if p in volumes.columns]

    price_jm1 = prices.iloc[t - 1][cols_prices] if cols_prices else pd.Series(dtype=float)
    vol_win = volumes.iloc[t - lookback_days:t][cols_vols] if cols_vols else pd.DataFrame()
    avg_vol = vol_win.mean()
    coverage = vol_win.notna().sum() / lookback_days

    out = {}
    for p in permnos:
        entry = {"selected": 0, "current_price": np.nan, "avg_volume_usd": np.nan, "reason": "unknown"}

        if p not in prices.columns:
            entry["reason"] = "not_in_prices"
        elif p not in volumes.columns:
            entry["reason"] = "not_in_volumes"
        else:
            cp = price_jm1.get(p, np.nan)
            entry["current_price"] = float(cp) if pd.notna(cp) else np.nan

            if pd.isna(cp):
                entry["reason"] = "missing_price"
            elif cp <= 0:
                entry["reason"] = "invalid_price_negative"
            elif cp < min_price:
                entry["reason"] = f"price_too_low_{min_price}"
            elif cp > max_price:
                entry["reason"] = f"price_too_high_{max_price}"
            else:
                vmean = avg_vol.get(p, np.nan)
                cov = coverage.get(p, 0.0)
                entry["avg_volume_usd"] = float(vmean) if pd.notna(vmean) else np.nan

                if cov < min_volume_coverage:
                    entry["reason"] = f"insufficient_volume_data_{cov:.1%}"
                elif pd.isna(vmean):
                    entry["reason"] = "missing_volume"
                elif vmean >= min_volume_usd:
                    entry["selected"] = 1
                    entry["reason"] = "passed_liquidity"
                else:
                    entry["reason"] = f"volume_too_low_{min_volume_usd / 1_000_000:.1f}M"

        out[p] = entry
    return out


def market_cap_filter(market_caps, permnos, decision_date, n_percentile):
    """Select permnos in the bottom `n_percentile` of the day's market-cap distribution.

    Cross-sectional filter on `decision_date`: builds the same-day market-
    cap distribution and selects permnos at or below the quantile cutoff
    (e.g. 0.3 keeps the smallest 30%).

    Args:
        market_caps: DataFrame of market caps, index=dates, columns=permnos.
        permnos: Permnos to evaluate.
        decision_date: Decision date.
        n_percentile: Quantile cutoff in [0, 1]. 0.3 = bottom 30%.

    Returns:
        dict mapping permno -> {selected, market_cap, percentile, reason}.

    Raises:
        Any error other than `KeyError` on `decision_date` propagates.
    """
    metric_keys = ("market_cap", "percentile")

    try:
        market_caps.index.get_loc(decision_date)
    except KeyError:
        return _passthrough(permnos, metric_keys, "date_not_in_index")

    cols = [p for p in permnos if p in market_caps.columns]
    me_today = market_caps.loc[decision_date, cols] if cols else pd.Series(dtype=float)

    if me_today.dropna().empty:
        return _passthrough(permnos, metric_keys, "no_market_cap_data_passthrough")

    threshold = me_today.dropna().quantile(n_percentile)
    percentiles = me_today.rank(method="min", pct=True)
    selected = (me_today <= threshold) & me_today.notna() & (me_today.fillna(0) > 0)

    out = {}
    for p in permnos:
        entry = {"selected": 0, "market_cap": np.nan, "percentile": np.nan, "reason": "unknown"}

        if p not in market_caps.columns:
            entry["reason"] = "not_in_market_caps"
        else:
            val = me_today.get(p, np.nan)
            pct = percentiles.get(p, np.nan)
            entry["market_cap"] = float(val) if pd.notna(val) else np.nan
            entry["percentile"] = float(pct) if pd.notna(pct) else np.nan

            if pd.isna(val):
                entry["reason"] = "missing_market_cap"
            elif val <= 0:
                entry["reason"] = "invalid_market_cap_negative"
            elif selected.get(p, False):
                entry["selected"] = 1
                entry["reason"] = f"passed_small_cap_{n_percentile:.0%}"
            else:
                entry["reason"] = f"failed_too_large_{n_percentile:.0%}"

        out[p] = entry
    return out


def momentum_filter(prices, permnos, decision_date,
                    min_return_6m, min_return_12m, exclude_last_month):
    """Filter on 6M and 12M trailing returns with optional 1-month skip.

    With `exclude_last_month=True`, the anchor shifts back ~1 trading
    month to avoid the well-documented short-term reversal that
    contaminates raw equity momentum (Jegadeesh-Titman 12-2 / 6-2 style).
    The 12M return falls back to all available history when the panel is
    shorter than 12 months from the anchor.

    Args:
        prices: DataFrame of prices, index=dates, columns=permnos.
        permnos: Permnos to evaluate.
        decision_date: Decision date `t`.
        min_return_6m: Minimum trailing 6M return required.
        min_return_12m: Minimum trailing 12M return required.
        exclude_last_month: Skip the most recent month when anchoring.

    Returns:
        dict mapping permno -> {selected, return_6m, return_12m, reason}.

    Raises:
        Any error other than `KeyError` on `decision_date` propagates.
    """
    metric_keys = ("return_6m", "return_12m")

    try:
        t = prices.index.get_loc(decision_date)
    except KeyError:
        return _passthrough(permnos, metric_keys, "date_not_in_index")

    if exclude_last_month:
        if t < _DAYS_PER_MONTH + _DAYS_PER_6M:
            return _passthrough(permnos, metric_keys, "insufficient_history_passthrough")
        anchor = t - _DAYS_PER_MONTH
        idx_6m = anchor - _DAYS_PER_6M
        if anchor >= _DAYS_PER_YEAR:
            idx_12m = anchor - _DAYS_PER_YEAR
            period_desc = "12M standard"
        else:
            # Truncated 12M: panel shorter than a full year from the anchor.
            idx_12m = 0
            period_desc = f"{anchor}d (~{anchor / _DAYS_PER_MONTH:.1f}M)"
    else:
        if t < _DAYS_PER_YEAR:
            return _passthrough(permnos, metric_keys, "insufficient_history_passthrough")
        anchor = t - 1
        idx_6m = t - _DAYS_PER_6M
        idx_12m = t - _DAYS_PER_YEAR
        period_desc = "12M standard"

    cols = [p for p in permnos if p in prices.columns]
    if not cols:
        return _passthrough(permnos, metric_keys, "not_in_prices", selected=0)

    pa = prices.iloc[anchor][cols]
    p6 = prices.iloc[idx_6m][cols]
    p12 = prices.iloc[idx_12m][cols]

    missing_any = pa.isna() | p6.isna() | p12.isna()
    nonpos_any = (pa <= 0) | (p6 <= 0) | (p12 <= 0)

    ret6 = pa / p6 - 1.0
    ret12 = pa / p12 - 1.0
    selected = (ret6 >= min_return_6m) & (ret12 >= min_return_12m) & ~missing_any & ~nonpos_any

    out = {}
    for p in permnos:
        entry = {"selected": 0, "return_6m": np.nan, "return_12m": np.nan, "reason": "unknown"}

        if p not in prices.columns:
            entry["reason"] = "not_in_prices"
        else:
            r6 = ret6.get(p, np.nan)
            r12 = ret12.get(p, np.nan)
            entry["return_6m"] = float(r6) if pd.notna(r6) else np.nan
            entry["return_12m"] = float(r12) if pd.notna(r12) else np.nan

            if missing_any.get(p, True):
                entry["reason"] = "missing_prices"
            elif nonpos_any.get(p, True):
                entry["reason"] = "invalid_prices"
            elif selected.get(p, False):
                entry["selected"] = 1
                entry["reason"] = f"passed_momentum_{period_desc}"
            elif (r6 < min_return_6m) and (r12 < min_return_12m):
                entry["reason"] = f"failed_both_6m_12m_{period_desc}"
            elif r6 < min_return_6m:
                entry["reason"] = f"failed_6m_{period_desc}"
            else:
                entry["reason"] = f"failed_12m_{period_desc}"

        out[p] = entry
    return out


def volatility_filter(prices, permnos, decision_date,
                      max_vol_threshold, lookback_days, min_data_ratio,
                      min_n_returns=params.VOL_MIN_N_RETURNS):
    """Filter on annualized rolling volatility over a price window.

    Annualizes daily-return std by `sqrt(252)` (standard convention) and
    rejects names whose annualized vol exceeds `max_vol_threshold`. Names
    with insufficient data coverage or fewer than `min_n_returns` valid
    returns are rejected as well — the std would be too noisy otherwise.

    Args:
        prices: DataFrame of prices, index=dates, columns=permnos.
        permnos: Permnos to evaluate.
        decision_date: Decision date `t`.
        max_vol_threshold: Maximum acceptable annualized vol (e.g. 0.80).
        lookback_days: Length of the price window.
        min_data_ratio: Minimum fraction of non-NaN prices in the window.
        min_n_returns: Minimum count of valid returns to compute std.

    Returns:
        dict mapping permno -> {selected, vol_annualized, data_ratio, reason}.

    Raises:
        Any error other than `KeyError` on `decision_date` propagates.
    """
    metric_keys = ("vol_annualized", "data_ratio")

    try:
        t = prices.index.get_loc(decision_date)
    except KeyError:
        return _passthrough(permnos, metric_keys, "date_not_in_index")

    if t < lookback_days:
        return _passthrough(permnos, metric_keys, "insufficient_history_passthrough")

    cols = [p for p in permnos if p in prices.columns]
    if not cols:
        return _passthrough(permnos, metric_keys, "not_in_prices", selected=0)

    win = prices.iloc[t - lookback_days:t][cols]
    data_ratio = win.notna().sum() / lookback_days
    rets = win.pct_change(fill_method=None)
    n_returns = rets.count()
    vol_ann = rets.std() * np.sqrt(_DAYS_PER_YEAR)

    out = {}
    for p in permnos:
        entry = {"selected": 0, "vol_annualized": np.nan, "data_ratio": np.nan, "reason": "unknown"}

        if p not in prices.columns:
            entry["reason"] = "not_in_prices"
        else:
            dr = float(data_ratio.get(p, 0.0))
            nr = int(n_returns.get(p, 0))
            va = vol_ann.get(p, np.nan)
            entry["data_ratio"] = dr
            entry["vol_annualized"] = float(va) if pd.notna(va) else np.nan

            if dr < min_data_ratio:
                entry["reason"] = f"insufficient_data_{dr:.1%}"
            elif nr < min_n_returns:
                entry["reason"] = f"insufficient_returns_{nr}"
            elif pd.isna(va):
                entry["reason"] = "calculation_error_vol"
            elif va <= max_vol_threshold:
                entry["selected"] = 1
                entry["reason"] = "passed_volatility"
            else:
                entry["reason"] = f"vol_too_high_{va:.1%}_vs_{max_vol_threshold:.1%}"

        out[p] = entry
    return out
