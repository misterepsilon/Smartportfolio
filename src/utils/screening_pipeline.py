"""Composite scoring and pipeline dispatch built on top of the primitive filters."""

import numpy as np
import pandas as pd

import params
from utils.screening import (
    liquidity_filter,
    market_cap_filter,
    momentum_filter,
    volatility_filter,
)


# (subscore name, source feature column, invert) — invert=True means
# "smaller raw value = higher score" (e.g. small-cap tilt, low-vol tilt).
_FEATURE_SPECS = (
    ("liquidity",    "avg_volume_usd", False),
    ("market_cap",   "market_cap",     True),
    ("momentum_6m",  "return_6m",      False),
    ("momentum_12m", "return_12m",     False),
    ("volatility",   "vol_annualized", True),
)


# Pipeline criterion → ordered tuple of stages. score_composite is handled
# separately because it returns scores rather than chaining selections.
_PIPELINE_STAGES = {
    "liquidity":  ("liq",),
    "market_cap": ("mc",),
    "momentum":   ("mom",),
    "volatility": ("vol",),
    "liquidity_market_cap":                     ("liq", "mc"),
    "liquidity_market_cap_momentum":            ("liq", "mc", "mom"),
    "liquidity_market_cap_momentum_volatility": ("liq", "mc", "mom", "vol"),
}


def _minmax(series, invert=False):
    """Min-max scale a cross-section to [0, 1], preserving NaN.

    Returns 0.5 for every valid (non-NaN) entry when the cross-section has
    no dispersion (all equal, or fewer than 2 valid values). NaN positions
    stay NaN so callers can apply an explicit penalty downstream.

    Args:
        series: Numeric Series.
        invert: If True, smaller raw values map to higher scores.

    Returns:
        Series in [0, 1] with NaN preserved.
    """
    s = series.astype(float)
    mn, mx = s.min(), s.max()
    if pd.isna(mn) or np.isclose(mx, mn):
        return s.where(s.isna(), 0.5)
    z = (s - mn) / (mx - mn)
    return 1.0 - z if invert else z


def _features_df(filter_dict, columns):
    """Convert a primitive-filter result dict to a permno-indexed feature frame."""
    df = pd.DataFrame.from_dict(filter_dict, orient="index")
    df.index.name = "permno"
    return df.reindex(columns=list(columns))


def composite_screening(prices, market_caps, volumes, permnos, decision_date, *,
                        weights=None, nan_penalty=0.0, top_n=30, min_score=None):
    """Score and rank permnos by a weighted combination of cross-sectional factors.

    Each primitive filter is run with its `params.*` defaults; the numeric
    outputs (avg dollar volume, market cap, 6/12M returns, annualized vol)
    are min-max scaled into [0, 1] sub-scores and combined linearly.

    Args:
        prices: DataFrame of prices, index=dates, columns=permnos.
        market_caps: DataFrame of market caps, same shape.
        volumes: DataFrame of USD volumes, same shape.
        permnos: Permnos to evaluate.
        decision_date: Decision date.
        weights: Mapping {feature → weight}. Accepts bare names
            (`liquidity`, `market_cap`, `momentum_6m`, `momentum_12m`,
            `volatility`) or the `w_*`-prefixed form used in
            `params.COMPOSITE_SCREENING_WEIGHTS`. Defaults to equal weights.
        nan_penalty: Sub-score assigned to NaN positions. 0.0 = full penalty.
        top_n: Keep the top-N by composite score. Set to None to use min_score.
        min_score: Alternative cutoff on composite score. Only used when
            `top_n` is None.

    Returns:
        DataFrame indexed by permno, sorted by composite_score descending,
        with raw features, sub-scores, `composite_score`, `rank`, `selected`.

    Raises:
        ValueError: If both `top_n` and `min_score` are None, or if active
            weights sum to zero.
    """
    if weights is None:
        weights = {sub: 1.0 for sub, _, _ in _FEATURE_SPECS}
    # Strip optional 'w_' prefix so params.COMPOSITE_SCREENING_WEIGHTS works as-is.
    weights = {k[2:] if k.startswith("w_") else k: v for k, v in weights.items()}

    liq = liquidity_filter(prices, volumes, permnos, decision_date,
                           params.LIQ_MIN_VOLUME_USD, params.LIQ_MIN_PRICE,
                           params.LIQ_MAX_PRICE, params.LIQ_LOOKBACK_DAYS)
    mc = market_cap_filter(market_caps, permnos, decision_date, params.N_PERCENTILES)
    mom = momentum_filter(prices, permnos, decision_date,
                          params.MOM_MIN_RETURN_6M, params.MOM_MIN_RETURN_12M,
                          params.MOM_EXCLUDE_LAST_MONTH)
    vol = volatility_filter(prices, permnos, decision_date,
                            params.VOL_MAX_THRESHOLD, params.VOL_LOOKBACK_DAYS,
                            params.VOL_MIN_DATA_RATIO)

    feats = pd.concat([
        _features_df(liq, ("current_price", "avg_volume_usd")),
        _features_df(mc,  ("market_cap", "percentile")),
        _features_df(mom, ("return_6m", "return_12m")),
        _features_df(vol, ("vol_annualized", "data_ratio")),
    ], axis=1).reindex(permnos)

    subscores = pd.DataFrame(index=feats.index)
    for sub_name, feat_col, invert in _FEATURE_SPECS:
        subscores[sub_name] = _minmax(feats[feat_col], invert=invert).fillna(nan_penalty)

    active = [c for c in subscores.columns if c in weights]
    w = np.array([weights[c] for c in active], dtype=float)
    if w.sum() <= 0:
        raise ValueError("composite weights sum to zero across the active features")
    w = w / w.sum()
    subscores["composite_score"] = subscores[active].values @ w

    # Prefix sub-score column names so they don't collide with raw features
    # of the same name (e.g. "market_cap" appears in both feats and subscores).
    subscores = subscores.rename(columns={s: f"score_{s}" for s, _, _ in _FEATURE_SPECS})
    out = pd.concat([feats, subscores], axis=1).sort_values("composite_score", ascending=False)
    out["rank"] = out["composite_score"].rank(ascending=False, method="dense")

    if top_n is None and min_score is None:
        raise ValueError("must specify either top_n or min_score")
    if top_n is not None:
        out["selected"] = out["rank"] <= top_n
    else:
        out["selected"] = out["composite_score"] >= float(min_score)
    return out


def _run_stage(stage, prices, volumes, market_caps, permnos, decision_date):
    """Run one filter stage with its params.* defaults and return selected permnos."""
    if stage == "liq":
        result = liquidity_filter(prices, volumes, permnos, decision_date,
                                  params.LIQ_MIN_VOLUME_USD, params.LIQ_MIN_PRICE,
                                  params.LIQ_MAX_PRICE, params.LIQ_LOOKBACK_DAYS)
    elif stage == "mc":
        result = market_cap_filter(market_caps, permnos, decision_date, params.N_PERCENTILES)
    elif stage == "mom":
        result = momentum_filter(prices, permnos, decision_date,
                                 params.MOM_MIN_RETURN_6M, params.MOM_MIN_RETURN_12M,
                                 params.MOM_EXCLUDE_LAST_MONTH)
    elif stage == "vol":
        result = volatility_filter(prices, permnos, decision_date,
                                   params.VOL_MAX_THRESHOLD, params.VOL_LOOKBACK_DAYS,
                                   params.VOL_MIN_DATA_RATIO)
    else:
        raise ValueError(f"unknown stage: {stage!r}")
    return [p for p, info in result.items() if info.get("selected", 0) == 1]


def select_assets_by_criteria(prices, market_caps, volumes, validated_available,
                              decision_date, criteria):
    """Apply a screening pipeline by name and return the selected permnos.

    Single-stage criteria run one filter; chained criteria pipe each
    stage's output as the universe for the next; `score_composite` runs
    the full composite scoring with `params.COMPOSITE_*` defaults.

    Args:
        prices: DataFrame of prices, index=dates, columns=permnos.
        market_caps: DataFrame of market caps, same shape.
        volumes: DataFrame of USD volumes, same shape.
        validated_available: Initial permno universe.
        decision_date: Decision date.
        criteria: One of "liquidity", "market_cap", "momentum",
            "volatility", "liquidity_market_cap",
            "liquidity_market_cap_momentum",
            "liquidity_market_cap_momentum_volatility",
            "score_composite".

    Returns:
        list of permnos selected by the pipeline.

    Raises:
        ValueError: If `criteria` is not a recognised preset.
    """
    if criteria == "score_composite":
        scores = composite_screening(
            prices, market_caps, volumes, validated_available, decision_date,
            weights=params.COMPOSITE_SCREENING_WEIGHTS,
            nan_penalty=params.COMPOSITE_SCORE_NAN_PENALTY,
            top_n=params.COMPOSITE_SCORE_TOP_N,
        )
        return scores.index[scores["selected"]].tolist()

    if criteria not in _PIPELINE_STAGES:
        valid = sorted(set(_PIPELINE_STAGES) | {"score_composite"})
        raise ValueError(f"unknown criteria: {criteria!r} (expected one of {valid})")

    permnos = validated_available
    for stage in _PIPELINE_STAGES[criteria]:
        permnos = _run_stage(stage, prices, volumes, market_caps, permnos, decision_date)
    return permnos
