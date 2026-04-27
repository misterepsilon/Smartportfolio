import pandas as pd

import params


def compute_portfolio_returns(
    panel,
    permnos=None,
    weights="equal",
    *,
    return_col="ret_combined",
    renormalize_on_missing=True,
):
    """Compute a daily portfolio return series from a long-format CRSP panel.

    Pivots the panel once and reduces row-wise via pandas' weighted-mean
    idiom — no Python loop over dates. The default return column splices
    delisting returns so the series is not biased upward when names exit.

    Args:
        panel: Long CRSP frame with one row per (date, permno). Required
            columns: `date`, `permno`, `return_col`, plus `me` when
            `weights == "market"`.
        permnos: Universe to include. None keeps every permno in `panel`.
        weights: "equal", "market", or a {permno: weight} mapping. Custom
            mappings need not sum to 1 — they are applied as raw weights
            and normalized by the row-wise denominator.
        return_col: CRSP return column. Defaults to "ret_combined" which
            includes `dlret` on the delisting day (avoids exit bias).
        renormalize_on_missing: If True, names with NaN returns drop out
            and surviving weights are rescaled to sum to 1 each day. If
            False, missing names act as cash (implicit zero return).

    Returns:
        Series indexed by date, named "portfolio_ret". NaN on days where
        no name in the universe has a valid return.

    Raises:
        ValueError: On unknown weights string or custom weights summing to
            zero across the panel universe.
    """
    df = panel if permnos is None else panel[panel["permno"].isin(permnos)]
    rets = (
        df.pivot(index="date", columns="permno", values=return_col)
          .sort_index()
    )

    if weights == "equal":
        # Equal-weight degenerates to the row mean. With renormalize=True,
        # pandas' default skipna gives the mean over surviving names; with
        # renormalize=False we divide by the full universe size so missing
        # names are absorbed as 0% (cash drag).
        if renormalize_on_missing:
            port = rets.mean(axis=1)
        else:
            port = rets.fillna(0.0).sum(axis=1) / rets.shape[1]
        return port.rename("portfolio_ret")

    if weights == "market":
        # shift(1): weight at t is built from market equity at t-1. Using
        # same-day cap would leak the day's return into the weight.
        w = (
            df.pivot(index="date", columns="permno", values="me")
              .reindex(index=rets.index, columns=rets.columns)
              .sort_index()
              .shift(1)
              .where(lambda x: x > 0)
        )
        num = (rets * w).sum(axis=1, min_count=1)
        denom = w.where(rets.notna()).sum(axis=1) if renormalize_on_missing else w.sum(axis=1)
    elif isinstance(weights, dict):
        w_vec = pd.Series(weights, dtype=float).reindex(rets.columns).fillna(0.0)
        if w_vec.sum() == 0:
            raise ValueError("custom weights sum to zero on the panel universe")
        # Series-on-DataFrame .mul(axis=1) broadcasts row-wise — no need to
        # materialize a (T, N) weights matrix.
        num = rets.mul(w_vec, axis=1).sum(axis=1, min_count=1)
        denom = (
            rets.notna().mul(w_vec, axis=1).sum(axis=1)
            if renormalize_on_missing
            else w_vec.sum()
        )
    else:
        raise ValueError(
            f"unknown weights scheme: {weights!r} (use 'equal', 'market', or a mapping)"
        )

    # When denom is per-row (Series), mask zero-denominator days back to
    # NaN so no-data days stay distinguishable from genuine 0% returns.
    if isinstance(denom, pd.Series):
        denom = denom.where(denom > 0)
    return (num / denom).rename("portfolio_ret")




def generate_rebalance_dates(return_index, frequency=params.REBALANCING_FREQUENCY):
    """Snap calendar rebalance dates to the next available trading day.

    Ideal dates falling on a non-trading day (weekend, holiday) advance
    forward to the next day present in `return_index`. Ideal dates beyond
    the last trading day in the index are dropped.

    Args:
        return_index: Sorted DatetimeIndex of available trading dates
            (typically the index of a return Series).
        frequency: One of "weekly" (Monday), "monthly", "quarterly",
            "yearly" — calendar period start.

    Returns:
        DatetimeIndex of trading-day rebalance dates, deduplicated.

    Raises:
        ValueError: If `frequency` is not one of the supported keys.
    """
    if frequency not in params.REBALANCE_FREQ_CODES:
        raise ValueError(
            f"unknown frequency: {frequency!r} "
            f"(expected one of {list(params.REBALANCE_FREQ_CODES)})"
        )
    ideal = pd.date_range(return_index.min(), return_index.max(),
                          freq=params.REBALANCE_FREQ_CODES[frequency])
    # searchsorted snaps each ideal date forward to the next trading day in
    # one vectorized pass; positions past the index end mean no future
    # trading day exists and are dropped.
    pos = return_index.searchsorted(ideal, side="left")
    pos = pos[pos < len(return_index)]
    return return_index[pos].unique()
