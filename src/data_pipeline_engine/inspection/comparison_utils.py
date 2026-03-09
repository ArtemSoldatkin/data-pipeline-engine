from __future__ import annotations


def mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def relative_change_pct(current: float, baseline: float | None) -> float | None:
    if baseline is None:
        return None
    denominator = abs(baseline) if baseline != 0 else 1.0
    return abs(current - baseline) / denominator * 100.0


def absolute_delta_pct(current: float, baseline: float | None) -> float | None:
    if baseline is None:
        return None
    return abs(current - baseline) * 100.0


def status_from_thresholds(
    value: float | None, warn_threshold: float | int | None, fail_threshold: float | int | None
) -> str:
    if value is None:
        return "no_baseline"
    if fail_threshold is not None and value >= float(fail_threshold):
        return "fail"
    if warn_threshold is not None and value >= float(warn_threshold):
        return "warn"
    return "pass"
