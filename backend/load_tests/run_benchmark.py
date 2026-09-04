#!/usr/bin/env python3
"""
High-Performance Headless Async Benchmark Runner for Event-Driven Order Platform.

Executes concurrent asynchronous load tests against Order & Inventory microservices,
measuring throughput (RPS), error rates, HTTP status code distributions, and
latency percentiles (p50, p90, p95, p99).

Zero-setup: Runs entirely from the command line using standard Python and httpx.
"""

import argparse
import asyncio
import time
import uuid
import sys
from typing import List, Dict, Any
from dataclasses import dataclass, field

try:
    import httpx
except ImportError:
    print("Error: 'httpx' is required to run this benchmark.")
    print("Install it using: pip install httpx")
    sys.exit(1)


@dataclass
class BenchmarkResult:
    status_code: int
    duration_ms: float
    error: str = ""


@dataclass
class BenchmarkSummary:
    total_requests: int
    successful_requests: int
    failed_requests: int
    total_duration_sec: float
    requests_per_sec: float
    latencies_ms: List[float] = field(default_factory=list)
    status_codes: Dict[int, int] = field(default_factory=dict)
    errors: Dict[str, int] = field(default_factory=dict)

    @property
    def p50(self) -> float:
        return self._percentile(50)

    @property
    def p90(self) -> float:
        return self._percentile(90)

    @property
    def p95(self) -> float:
        return self._percentile(95)

    @property
    def p99(self) -> float:
        return self._percentile(99)

    @property
    def min_latency(self) -> float:
        return min(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def max_latency(self) -> float:
        return max(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def mean_latency(self) -> float:
        return sum(self.latencies_ms) / len(self.latencies_ms) if self.latencies_ms else 0.0

    def _percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        k = (len(sorted_lat) - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, len(sorted_lat) - 1)
        d = k - f
        return sorted_lat[f] + (sorted_lat[c] - sorted_lat[f]) * d


async def execute_order_create(client: httpx.AsyncClient, base_url: str) -> BenchmarkResult:
    """Dispatches an idempotent order creation request."""
    idempotency_key = str(uuid.uuid4())
    customer_id = "c1010000-0000-0000-0000-000000000101"
    product_id = str(uuid.uuid4())

    payload = {
        "customer_id": customer_id,
        "items": [{"product_id": product_id, "quantity": 2}]
    }
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key
    }

    start = time.perf_counter()
    try:
        resp = await client.post(f"{base_url}/orders/", json=payload, headers=headers)
        duration_ms = (time.perf_counter() - start) * 1000.0
        return BenchmarkResult(status_code=resp.status_code, duration_ms=duration_ms)
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000.0
        return BenchmarkResult(status_code=0, duration_ms=duration_ms, error=type(e).__name__)


async def execute_order_lookup(client: httpx.AsyncClient, base_url: str) -> BenchmarkResult:
    """Dispatches a cached order lookup or health check."""
    start = time.perf_counter()
    try:
        resp = await client.get(f"{base_url}/health/ready")
        duration_ms = (time.perf_counter() - start) * 1000.0
        return BenchmarkResult(status_code=resp.status_code, duration_ms=duration_ms)
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000.0
        return BenchmarkResult(status_code=0, duration_ms=duration_ms, error=type(e).__name__)


async def execute_rate_limit_burst(client: httpx.AsyncClient, base_url: str) -> BenchmarkResult:
    """Fires requests against orders endpoint to stress test the sliding-window rate limiter."""
    start = time.perf_counter()
    try:
        resp = await client.get(f"{base_url}/orders/00000000-0000-0000-0000-000000000000")
        duration_ms = (time.perf_counter() - start) * 1000.0
        return BenchmarkResult(status_code=resp.status_code, duration_ms=duration_ms)
    except Exception as e:
        duration_ms = (time.perf_counter() - start) * 1000.0
        return BenchmarkResult(status_code=0, duration_ms=duration_ms, error=type(e).__name__)


async def worker(
    queue: asyncio.Queue,
    client: httpx.AsyncClient,
    base_url: str,
    scenario: str,
    results: List[BenchmarkResult]
):
    """Worker task draining the request queue concurrently."""
    while not queue.empty():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        if scenario == "orders":
            res = await execute_order_create(client, base_url)
        elif scenario == "lookup":
            res = await execute_order_lookup(client, base_url)
        elif scenario == "ratelimit":
            res = await execute_rate_limit_burst(client, base_url)
        else:
            # Mixed default scenario
            res = await execute_order_create(client, base_url)

        results.append(res)
        queue.task_done()


async def run_benchmark(
    base_url: str,
    concurrency: int,
    total_requests: int,
    scenario: str,
    timeout_sec: float
) -> BenchmarkSummary:
    queue: asyncio.Queue = asyncio.Queue()
    for _ in range(total_requests):
        queue.put_nowait(1)

    results: List[BenchmarkResult] = []

    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(timeout_sec)

    start_time = time.perf_counter()
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        workers = [
            asyncio.create_task(worker(queue, client, base_url, scenario, results))
            for _ in range(concurrency)
        ]
        await asyncio.gather(*workers)
    wall_clock = time.perf_counter() - start_time

    # Aggregate results
    latencies = [r.duration_ms for r in results if r.status_code > 0]
    status_codes: Dict[int, int] = {}
    errors: Dict[str, int] = {}
    success_count = 0
    fail_count = 0

    for r in results:
        if r.status_code > 0:
            status_codes[r.status_code] = status_codes.get(r.status_code, 0) + 1
            # HTTP 2xx or expected 429 (rate limit) count as successful handling
            if 200 <= r.status_code < 300 or r.status_code == 429:
                success_count += 1
            else:
                fail_count += 1
        else:
            fail_count += 1
            err_name = r.error or "UnknownError"
            errors[err_name] = errors.get(err_name, 0) + 1

    rps = len(results) / wall_clock if wall_clock > 0 else 0.0

    return BenchmarkSummary(
        total_requests=len(results),
        successful_requests=success_count,
        failed_requests=fail_count,
        total_duration_sec=wall_clock,
        requests_per_sec=rps,
        latencies_ms=latencies,
        status_codes=status_codes,
        errors=errors
    )


def print_report(summary: BenchmarkSummary, base_url: str, concurrency: int, scenario: str):
    """Outputs an enterprise ASCII summary table of the benchmark results."""
    divider = "=" * 65
    thin_divider = "-" * 65

    print("\n" + divider)
    print("       EVENT-DRIVEN ORDER PLATFORM - PERFORMANCE BENCHMARK       ")
    print(divider)
    print(f"Target URL:          {base_url}")
    print(f"Scenario:            {scenario}")
    print(f"Concurrency Level:   {concurrency} virtual workers")
    print(f"Total Requests:      {summary.total_requests}")
    print(f"Completed in:        {summary.total_duration_sec:.3f} seconds")
    print(f"Throughput:          {summary.requests_per_sec:.2f} req/sec (RPS)")
    print(thin_divider)
    print("LATENCY DISTRIBUTION (Milliseconds):")
    print(f"  Min:               {summary.min_latency:8.2f} ms")
    print(f"  Mean:              {summary.mean_latency:8.2f} ms")
    print(f"  50th Percentile:   {summary.p50:8.2f} ms (p50)")
    print(f"  90th Percentile:   {summary.p90:8.2f} ms (p90)")
    print(f"  95th Percentile:   {summary.p95:8.2f} ms (p95)")
    print(f"  99th Percentile:   {summary.p99:8.2f} ms (p99)")
    print(f"  Max:               {summary.max_latency:8.2f} ms")
    print(thin_divider)
    print("HTTP STATUS CODES:")
    if summary.status_codes:
        for code, count in sorted(summary.status_codes.items()):
            pct = (count / summary.total_requests) * 100.0
            print(f"  HTTP {code}:          {count:6d} ({pct:5.1f}%)")
    else:
        print("  No successful HTTP responses received.")

    if summary.errors:
        print(thin_divider)
        print("CONNECTION / CLIENT ERRORS:")
        for err, count in sorted(summary.errors.items()):
            pct = (count / summary.total_requests) * 100.0
            print(f"  {err:18s}: {count:6d} ({pct:5.1f}%)")

    print(thin_divider)
    # SLA evaluation
    p95_ok = summary.p95 <= 200.0 if summary.latencies_ms else False
    err_rate = (summary.failed_requests / summary.total_requests) * 100.0 if summary.total_requests else 100.0
    sla_ok = p95_ok and err_rate < 5.0

    print("PRODUCTION SLA CRITERIA:")
    print(f"  p95 < 200ms:       {'[PASS]' if p95_ok else '[FAIL]'} ({summary.p95:.1f}ms)")
    print(f"  Error Rate < 5%:   {'[PASS]' if err_rate < 5.0 else '[FAIL]'} ({err_rate:.1f}%)")
    print(f"  Overall Status:    {'READY FOR PRODUCTION' if sla_ok else 'ATTENTION NEEDED'}")
    print(divider + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Headless Async Benchmark for Event-Driven Order Platform"
    )
    parser.add_argument(
        "--target-url",
        default="http://localhost:8000",
        help="Base URL of service under test (default: http://localhost:8000)"
    )
    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=20,
        help="Number of concurrent asynchronous workers (default: 20)"
    )
    parser.add_argument(
        "-n", "--requests",
        type=int,
        default=100,
        help="Total number of requests to execute (default: 100)"
    )
    parser.add_argument(
        "--scenario",
        choices=["orders", "lookup", "ratelimit"],
        default="orders",
        help="Benchmark scenario to execute (default: orders)"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Request timeout in seconds (default: 5.0)"
    )

    args = parser.parse_args()

    print(f"Starting benchmark: {args.requests} requests across {args.concurrency} concurrent workers...")
    print(f"Scenario: '{args.scenario}' targeting {args.target_url}")

    summary = asyncio.run(
        run_benchmark(
            base_url=args.target_url.rstrip("/"),
            concurrency=args.concurrency,
            total_requests=args.requests,
            scenario=args.scenario,
            timeout_sec=args.timeout
        )
    )

    print_report(summary, args.target_url, args.concurrency, args.scenario)


if __name__ == "__main__":
    main()
