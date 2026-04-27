import numpy as np
import pandas as pd
import params


def sharpe_ratio(weights, mean, covariance_matrix, rf=params.RISK_FREE_RATE_DAILY):
    """Compute the in-sample Sharpe ratio of a weighted portfolio.

    All inputs are expected on a **daily** basis: `mean` and
    `covariance_matrix` are estimated from daily returns and `rf` defaults
    to the daily risk-free rate. Annualize the result downstream if needed.

    Args:
        weights: Portfolio weights, shape (N,).
        mean: Per-asset expected daily return, shape (N,).
        covariance_matrix: Daily return covariance, shape (N, N).
        rf: Daily risk-free rate.

    Returns:
        Daily Sharpe ratio.
    """
    # +1e-12: numerical guard against zero/near-zero variance (e.g. a
    # single-asset corner solution or a degenerate covariance) which would
    # otherwise produce inf or NaN and break optimizers downstream.
    vol = np.sqrt(weights.T @ covariance_matrix @ weights) + 1e-12
    return ((weights @ mean) - rf) / vol


def drawdown_series(wealth):
    """Compute the running drawdown of a wealth/price series.

    Drawdown at time t is `wealth_t / max(wealth_0..t) - 1`: 0 at every
    new high-water mark, negative elsewhere. Accepts a pandas Series or
    array-like; the returned Series shares the input's index when present.

    Args:
        wealth: Wealth, NAV, or price series. From period returns r_t,
            build wealth via `(1 + r).fillna(0).cumprod()`. From cumulative
            returns c_t, use `1 + c`.

    Returns:
        pd.Series of drawdown values (≤ 0), aligned to the input index.
        NaN at any row where the running max is non-positive (see Notes).

    Notes:
        Wealth built from CRSP returns is always ≥ 0, so a mid-series
        wipeout to 0 still has a positive earlier peak and produces a
        legitimate -1.0 drawdown — no special handling needed. The guard
        below covers a narrower case: a series that is non-positive from
        the start (e.g. negative-price futures like Brent in April 2020),
        where the formula `s / running_max - 1` would otherwise return
        spurious *positive* values.
    """
    s = pd.Series(wealth).astype(float)
    if s.empty:
        return s

    running_max = s.cummax()
    with np.errstate(divide="ignore", invalid="ignore"):
        dd = s / running_max - 1.0
    # Negative running_max flips the sign of the ratio and would report
    # a "positive drawdown" — fail loud with NaN instead.
    dd[running_max <= 0] = np.nan
    return dd


def max_drawdown(returns, weights):
    """Compute the worst peak-to-trough drawdown of a weighted portfolio.

    Builds the daily portfolio wealth path from `returns @ weights` and
    returns the minimum of `drawdown_series` on that path.

    Args:
        returns: Historical daily returns matrix, shape (T, N).
        weights: Portfolio weights, shape (N,).

    Returns:
        Maximum drawdown as a non-positive float (e.g. -0.23 for -23%).
    """
    rp = returns @ weights
    wealth = np.cumprod(rp + 1)
    return drawdown_series(wealth).min()


def sharpe_with_dd_penalty(
    weights,
    mean,
    covariance_matrix,
    returns_hist,
    rf=params.RISK_FREE_RATE_DAILY,
    dd_cap=params.MAX_DRAWDOWN,
    rho=params.RHO_DRAWDOWN,
    use_drawdown=params.USE_DRAWDOWN_PENALTY,
):
    """Negative Sharpe with optional quadratic drawdown penalty (for minimization).

    Designed to be passed as the objective to a minimizer: returns
    -Sharpe so the optimizer maximizes Sharpe, plus a penalty that grows
    quadratically once realized drawdown breaches `dd_cap`. All
    return-based inputs are daily; see `sharpe_ratio`.

    Args:
        weights: Portfolio weights, shape (N,).
        mean: Per-asset expected daily return, shape (N,).
        covariance_matrix: Daily return covariance, shape (N, N).
        returns_hist: Historical daily returns matrix, shape (T, N), used
            only for the realized-drawdown computation.
        rf: Daily risk-free rate.
        dd_cap: Drawdown cap as a non-positive float (e.g. -0.15). The
            penalty activates only when realized MDD is *worse* than this.
        rho: Penalty intensity. Quadratic growth so a small breach is
            cheap and a large one is sharply discouraged.
        use_drawdown: If False, returns -Sharpe with no penalty.

    Returns:
        -Sharpe + penalty, suitable for `scipy.optimize.minimize`.
    """
    neg_sharpe = -sharpe_ratio(weights, mean, covariance_matrix, rf)
    if use_drawdown:
        mdd = max_drawdown(returns_hist, weights)
        # Both dd_cap and mdd are non-positive. The breach magnitude is
        # `dd_cap - mdd` (positive when mdd is worse than the cap, e.g.
        # cap=-0.15, mdd=-0.20 -> breach=+0.05); clipped at 0 so within-cap
        # solutions incur no penalty.
        violation = max(0.0, dd_cap - mdd)
        # Quadratic (vs linear) so the gradient near the cap is zero —
        # solutions sitting just inside the cap aren't pushed away, but
        # large breaches blow up fast.
        penalty = rho * (violation ** 2)
        return neg_sharpe + penalty
    return neg_sharpe
