#!/usr/bin/env python3
"""
A/B Testing System for AI Music Empire
- Title / Tags / Thumbnail A/B tests
- Z-test significance testing
- Result tracking and reporting
"""

import json
import math
import os
from datetime import datetime
from typing import Literal

AB_TESTS_PATH = "data/ab_tests.json"


def z_test_proportions(successes_a, trials_a, successes_b, trials_b):
    """Two-proportion z-test for CTR comparison."""
    if trials_a == 0 or trials_b == 0:
        return {"z_score": 0, "p_value": 1.0, "significant": False, "error": "insufficient data"}
    p_a = successes_a / trials_a
    p_b = successes_b / trials_b
    p_pool = (successes_a + successes_b) / (trials_a + trials_b)
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / trials_a + 1 / trials_b))
    if se == 0:
        return {"z_score": 0, "p_value": 1.0, "significant": False}
    z = (p_a - p_b) / se
    p_value = 2 * (1 - _normal_cdf(abs(z)))
    return {
        "z_score": round(z, 4), "p_value": round(p_value, 6),
        "significant": p_value < 0.05, "rate_a": round(p_a, 4),
        "rate_b": round(p_b, 4),
        "lift_percent": round((p_b - p_a) / p_a * 100, 2) if p_a > 0 else 0,
    }


def z_test_means(mean_a, std_a, n_a, mean_b, std_b, n_b):
    """Two-sample z-test for means (e.g., average watch time)."""
    if n_a < 2 or n_b < 2:
        return {"z_score": 0, "p_value": 1.0, "significant": False, "error": "insufficient data"}
    se = math.sqrt((std_a ** 2) / n_a + (std_b ** 2) / n_b)
    if se == 0:
        return {"z_score": 0, "p_value": 1.0, "significant": False}
    z = (mean_a - mean_b) / se
    p_value = 2 * (1 - _normal_cdf(abs(z)))
    return {
        "z_score": round(z, 4), "p_value": round(p_value, 6),
        "significant": p_value < 0.05, "mean_a": round(mean_a, 4),
        "mean_b": round(mean_b, 4), "difference": round(mean_b - mean_a, 4),
    }


def _normal_cdf(x):
    """Approximation of the standard normal CDF (Abramowitz & Stegun)."""
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    return 0.5 * (1.0 + sign * y)


class ABTest:
    """Represents a single A/B test experiment."""

    def __init__(self, test_id, test_type, variant_a, variant_b, channel="", genre=""):
        self.test_id = test_id
        self.test_type = test_type
        self.variant_a = variant_a
        self.variant_b = variant_b
        self.channel = channel
        self.genre = genre
        self.created_at = datetime.utcnow().isoformat()
        self.status = "running"
        self.results_a = {"impressions": 0, "clicks": 0, "views": 0, "watch_time_total": 0}
        self.results_b = {"impressions": 0, "clicks": 0, "views": 0, "watch_time_total": 0}

    def update_results(self, variant, metrics):
        target = self.results_a if variant == "a" else self.results_b
        for key, value in metrics.items():
            if key in target:
                target[key] += value

    def evaluate_ctr(self):
        result = z_test_proportions(
            self.results_a["clicks"], self.results_a["impressions"],
            self.results_b["clicks"], self.results_b["impressions"],
        )
        result["metric"] = "ctr"
        result["test_id"] = self.test_id
        return result

    def evaluate_watch_time(self):
        n_a = self.results_a["views"] or 1
        n_b = self.results_b["views"] or 1
        mean_a = self.results_a["watch_time_total"] / n_a
        mean_b = self.results_b["watch_time_total"] / n_b
        result = z_test_means(mean_a, mean_a * 0.4, n_a, mean_b, mean_b * 0.4, n_b)
        result["metric"] = "avg_watch_time"
        result["test_id"] = self.test_id
        return result

    def get_winner(self):
        ctr_eval = self.evaluate_ctr()
        wt_eval = self.evaluate_watch_time()
        winner = "inconclusive"
        if ctr_eval.get("significant"):
            winner = "B" if ctr_eval.get("rate_b", 0) > ctr_eval.get("rate_a", 0) else "A"
        elif wt_eval.get("significant"):
            winner = "B" if wt_eval.get("mean_b", 0) > wt_eval.get("mean_a", 0) else "A"
        return {
            "test_id": self.test_id, "test_type": self.test_type, "winner": winner,
            "ctr_analysis": ctr_eval, "watch_time_analysis": wt_eval,
            "variant_a": self.variant_a, "variant_b": self.variant_b,
        }

    def to_dict(self):
        return {
            "test_id": self.test_id, "test_type": self.test_type,
            "variant_a": self.variant_a, "variant_b": self.variant_b,
            "channel": self.channel, "genre": self.genre,
            "created_at": self.created_at, "status": self.status,
            "results_a": self.results_a, "results_b": self.results_b,
        }

    @classmethod
    def from_dict(cls, data):
        test = cls(data["test_id"], data["test_type"], data["variant_a"], data["variant_b"],
                   data.get("channel", ""), data.get("genre", ""))
        test.created_at = data.get("created_at", "")
        test.status = data.get("status", "running")
        test.results_a = data.get("results_a", test.results_a)
        test.results_b = data.get("results_b", test.results_b)
        return test


class ABTestRegistry:
    """Manage all A/B tests."""

    def __init__(self):
        self.tests = {}
        self._load()

    def _load(self):
        data = _load_json(AB_TESTS_PATH)
        if isinstance(data, list):
            for item in data:
                test = ABTest.from_dict(item)
                self.tests[test.test_id] = test

    def save(self):
        os.makedirs(os.path.dirname(AB_TESTS_PATH), exist_ok=True)
        with open(AB_TESTS_PATH, "w") as f:
            json.dump([t.to_dict() for t in self.tests.values()], f, indent=2)

    def create_test(self, **kwargs):
        test = ABTest(**kwargs)
        self.tests[test.test_id] = test
        self.save()
        return test

    def evaluate_all(self):
        results = []
        for test in self.tests.values():
            if test.status == "running":
                winner = test.get_winner()
                results.append(winner)
                if winner["winner"] != "inconclusive":
                    test.status = "completed"
        self.save()
        return results

    def summary(self):
        total = len(self.tests)
        running = sum(1 for t in self.tests.values() if t.status == "running")
        completed = sum(1 for t in self.tests.values() if t.status == "completed")
        return {"total": total, "running": running, "completed": completed}


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def run_ab_evaluation():
    print(f"A/B Testing evaluation at {datetime.utcnow().isoformat()}")
    registry = ABTestRegistry()
    summary = registry.summary()
    print(f"  Total: {summary['total']} | Running: {summary['running']} | Completed: {summary['completed']}")
    results = registry.evaluate_all()
    for r in results:
        status = "WINNER" if r["winner"] != "inconclusive" else "PENDING"
        print(f"  [{status}] {r['test_id']} ({r['test_type']}): winner={r['winner']}")
    print(f"Evaluation complete. {len(results)} tests evaluated.")
    return results


if __name__ == "__main__":
    run_ab_evaluation()
