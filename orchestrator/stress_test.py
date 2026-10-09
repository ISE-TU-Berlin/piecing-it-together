#!/usr/bin/env python3
"""
Stress testing tool for gRPC services (ShippingService, SyntheticService).

Features:
- Configurable parallel connections
- Configurable concurrent requests per connection
- Configurable total request count
- Real-time progress tracking
- Detailed performance metrics
"""

import argparse
import asyncio
import csv
from threading import Event
import time
import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, List, Dict, Optional
import random

import grpc
import demo_pb2 
import demo_pb2_grpc

PRODUCTS = [
    "0PUK6V6EV0", "1YMWWN1N4O", "2ZYFJ3GM2N", "66VCHSJNUP",
    "6E92ZMYYFZ", "9SIQT8TOJO", "L9ECAV7KIM", "LS4PSXUNUM", "OLJCESPC7Z",
]


@dataclass
class StressTestConfig:
    """Configuration for stress test."""
    host: str = "localhost:50051"
    total_requests: int = 1000
    parallel_connections: int = 6
    requests_per_connection: int = 6 
    rpc_method: str = "ShipOrder"  # GetQuote or ShipOrder (ShippingService only)
    timeout: float = 60
    log_responses: bool = False
    max_logged_responses: int = 5
    window_metrics_csv: Optional[str] = None
    # A gRPC channel multiplexes every RPC over a single HTTP/2 connection that,
    # once established, stays pinned to one backend pod. To let pods that scale up
    # mid-run actually receive traffic, recycle each channel periodically (forcing
    # a new TCP connection that kube-proxy may route elsewhere) and on transport
    # failure. recycle_after_requests=0 disables proactive recycling.
    recycle_after_requests: int = 72
    reconnect_on_error: bool = True
    error_backoff: float = 0.1  # brief pause after a failed request (was a hard 1s)
    # A gRPC channel multiplexes every RPC over a single HTTP/2 connection that,
    # once established, stays pinned to one backend pod. To let pods that scale up
    # mid-run actually receive traffic, recycle each channel periodically (forcing
    # a new TCP connection that kube-proxy may route elsewhere) and on transport
    # failure. recycle_after_requests=0 disables proactive recycling.
    recycle_after_requests: int = 100
    reconnect_on_error: bool = True
    error_backoff: float = 0.1  # brief pause after a failed request (was a hard 1s)


@dataclass
class WindowMetric:
    """Per-second metrics snapshot."""
    timestamp: int
    user_count: int
    requests_per_second: float
    failures_per_second: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    total_request_count: int
    total_failure_count: int
    total_median_response_time_ms: float
    total_average_response_time_ms: float


@dataclass
class TestMetrics:
    """Metrics collected during stress test."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    latencies: List[float] = field(default_factory=list)
    errors: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    start_time: float = 0
    end_time: float = 0
    sample_responses: List[str] = field(default_factory=list)
    window_metrics: List[WindowMetric] = field(default_factory=list)

    _active_window_timestamp: Optional[int] = field(default=None, init=False, repr=False)
    _active_window_requests: int = field(default=0, init=False, repr=False)
    _active_window_failures: int = field(default=0, init=False, repr=False)
    _active_window_latencies: List[float] = field(default_factory=list, init=False, repr=False)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time else 0

    @property
    def success_rate(self) -> float:
        return (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0

    @property
    def avg_latency(self) -> float:
        return sum(self.latencies) / len(self.latencies) if self.latencies else 0

    @property
    def min_latency(self) -> float:
        return min(self.latencies) if self.latencies else 0

    @property
    def max_latency(self) -> float:
        return max(self.latencies) if self.latencies else 0

    @property
    def p50_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        return sorted_latencies[len(sorted_latencies) // 2]

    @property
    def p95_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[idx]

    @property
    def p99_latency(self) -> float:
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[idx]

    @property
    def requests_per_second(self) -> float:
        return self.total_requests / self.duration if self.duration > 0 else 0

    @staticmethod
    def _percentile(values: List[float], percentile: float) -> float:
        if not values:
            return 0
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * percentile)
        if idx >= len(sorted_values):
            idx = len(sorted_values) - 1
        return sorted_values[idx]

    def _finalize_current_window(self, user_count: int):
        if self._active_window_timestamp is None:
            return

        self.window_metrics.append(
            WindowMetric(
                timestamp=self._active_window_timestamp,
                user_count=user_count,
                requests_per_second=float(self._active_window_requests),
                failures_per_second=float(self._active_window_failures),
                p50_latency_ms=self._percentile(self._active_window_latencies, 0.50) * 1000,
                p95_latency_ms=self._percentile(self._active_window_latencies, 0.95) * 1000,
                p99_latency_ms=self._percentile(self._active_window_latencies, 0.99) * 1000,
                total_request_count=self.total_requests,
                total_failure_count=self.failed_requests,
                total_median_response_time_ms=self.p50_latency * 1000,
                total_average_response_time_ms=self.avg_latency * 1000,
            )
        )

        self._active_window_requests = 0
        self._active_window_failures = 0
        self._active_window_latencies.clear()

    def record_request(self, success: bool, latency: float, error: str, event_time: float, user_count: int):
        self.total_requests += 1
        self.latencies.append(latency)

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            self.errors[error] += 1

        second_bucket = int(event_time)
        if self._active_window_timestamp is None:
            self._active_window_timestamp = second_bucket

        if second_bucket > self._active_window_timestamp:
            while self._active_window_timestamp < second_bucket:
                self._finalize_current_window(user_count)
                self._active_window_timestamp += 1

        self._active_window_requests += 1
        if not success:
            self._active_window_failures += 1
        self._active_window_latencies.append(latency)

    def finalize_windows(self, end_time: float, user_count: int):
        if self._active_window_timestamp is None:
            return

        self._finalize_current_window(user_count)
        end_second = int(end_time)
        while self._active_window_timestamp is not None and self._active_window_timestamp < end_second:
            self._active_window_timestamp += 1
            self._finalize_current_window(user_count)

    def to_dict(self) -> dict:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": self.success_rate,
            "avg_latency": self.avg_latency,
            "min_latency": self.min_latency,
            "max_latency": self.max_latency,
            "p50_latency": self.p50_latency,
            "p95_latency": self.p95_latency,
            "p99_latency": self.p99_latency,
            "requests_per_second": self.requests_per_second,
            "errors": dict(self.errors),
            "duration": self.duration,
            "sample_responses": list(self.sample_responses),
            "window_metrics": [
                {
                    "timestamp": m.timestamp,
                    "user_count": m.user_count,
                    "requests_per_second": m.requests_per_second,
                    "failures_per_second": m.failures_per_second,
                    "p50_latency_ms": m.p50_latency_ms,
                    "p95_latency_ms": m.p95_latency_ms,
                    "p99_latency_ms": m.p99_latency_ms,
                    "total_request_count": m.total_request_count,
                    "total_failure_count": m.total_failure_count,
                    "total_median_response_time_ms": m.total_median_response_time_ms,
                    "total_average_response_time_ms": m.total_average_response_time_ms,
                }
                for m in self.window_metrics
            ],
        }


class ManagedConnection:
    """A single gRPC channel + stub that can be recycled on demand.

    Recycling swaps in a fresh channel for *future* requests and closes the old
    one with a grace period, so in-flight RPCs (bounded by the per-request
    timeout) drain instead of being cancelled.
    """

    def __init__(self, tester: "StressTester"):
        self._tester = tester
        self._channel: Optional[grpc.aio.Channel] = None
        self.stub = None
        self._uses = 0
        self._lock = asyncio.Lock()
        self._drain_tasks: set = set()

    def connect(self):
        self._channel = grpc.aio.insecure_channel(self._tester.config.host)
        self.stub = self._tester.create_stub(self._channel)
        self._uses = 0

    async def acquire_stub(self):
        """Return the stub to use for the next request, recycling first if the
        proactive request budget is exhausted (so the triggering request lands
        on the fresh connection)."""
        async with self._lock:
            recycle_after = self._tester.config.recycle_after_requests
            if self._channel is None:
                self.connect()
            elif recycle_after and self._uses >= recycle_after:
                self._recycle_locked()
            self._uses += 1
            return self.stub

    async def report_failure(self):
        """Drop the (likely dead/slow/misrouted) channel so the next request
        reconnects and may reach a healthier pod."""
        if not self._tester.config.reconnect_on_error:
            return
        async with self._lock:
            self._recycle_locked()

    def _recycle_locked(self):
        old = self._channel
        self.connect()
        if old is not None:
            t = asyncio.create_task(self._drain_close(old, self._tester.config.timeout))
            self._drain_tasks.add(t)
            t.add_done_callback(self._drain_tasks.discard)

    async def _drain_close(self, channel, grace: float):
        try:
            await channel.close(grace=grace)
        except Exception:
            pass

    async def close(self):
        async with self._lock:
            if self._channel is not None:
                try:
                    await self._channel.close()
                except Exception:
                    pass
            self._channel = None
            self.stub = None
        if self._drain_tasks:
            await asyncio.gather(*self._drain_tasks, return_exceptions=True)


class StressTester(ABC):
    """Base class for gRPC service stress testers."""

    service_name: str = "Service"

    # Transport-level failures worth reconnecting on. App-level errors (e.g.
    # INVALID_ARGUMENT, NOT_FOUND) do not warrant dropping the channel.
    RECONNECT_CODES = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }

    # Transport-level failures worth reconnecting on. App-level errors (e.g.
    # INVALID_ARGUMENT, NOT_FOUND) do not warrant dropping the channel.
    RECONNECT_CODES = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }

    def __init__(self, config: StressTestConfig):
        self.config = config
        self.metrics = TestMetrics()
        self.connections: List[ManagedConnection] = []
        self.connections: List[ManagedConnection] = []

    @abstractmethod
    def create_stub(self, channel) -> object:
        """Create a gRPC stub for the target service."""

    @abstractmethod
    async def call_rpc(self, stub) -> object:
        """Perform a single RPC call and return the response."""

    async def setup_connections(self):
        print(f"Setting up {self.config.parallel_connections} connections to {self.config.host}...")
        self.connections = []
        self.connections = []
        for _ in range(self.config.parallel_connections):
            conn = ManagedConnection(self)
            conn.connect()
            self.connections.append(conn)
            conn = ManagedConnection(self)
            conn.connect()
            self.connections.append(conn)
        print(f"✓ All connections established")

    async def teardown_connections(self):
        await asyncio.gather(*(c.close() for c in self.connections), return_exceptions=True)
        self.connections = []
        await asyncio.gather(*(c.close() for c in self.connections), return_exceptions=True)
        self.connections = []

    async def make_request(self, stub) -> tuple[bool, float, str, bool]:
        """Returns (success, latency, error, should_reconnect)."""
        start_time = time.perf_counter()
        try:
            response = await self.call_rpc(stub)
            latency = time.perf_counter() - start_time
            await self._log_response(response)
            return True, latency, "", False
            return True, latency, "", False
        except grpc.RpcError as e:
            latency = time.perf_counter() - start_time
            if self.config.error_backoff > 0:
                await asyncio.sleep(self.config.error_backoff)
            return False, latency, f"{e.code().name}: {e.details()}", e.code() in self.RECONNECT_CODES
            if self.config.error_backoff > 0:
                await asyncio.sleep(self.config.error_backoff)
            return False, latency, f"{e.code().name}: {e.details()}", e.code() in self.RECONNECT_CODES
        except Exception as e:
            latency = time.perf_counter() - start_time
            if self.config.error_backoff > 0:
                await asyncio.sleep(self.config.error_backoff)
            return False, latency, str(e), True
            if self.config.error_backoff > 0:
                await asyncio.sleep(self.config.error_backoff)
            return False, latency, str(e), True

    async def _log_response(self, response) -> None:
        if not self.config.log_responses:
            return
        if len(self.metrics.sample_responses) >= self.config.max_logged_responses:
            return
        preview = str(response).replace("\n", " ").strip()
        if len(preview) > 400:
            preview = preview[:400] + "..."
        self.metrics.sample_responses.append(f"{response.__class__.__name__}: {preview}")

    async def run_connection_worker(
        self,
        conn: ManagedConnection,
        requests_to_make: Optional[int],
        progress_queue: asyncio.Queue,
        stop_event: Optional[Event] = None,
    ):
        semaphore = asyncio.Semaphore(self.config.requests_per_connection)

        async def bounded_request():
            async with semaphore:
                stub = await conn.acquire_stub()
                success, latency, error, should_reconnect = await self.make_request(stub)
                if should_reconnect:
                    await conn.report_failure()
                await progress_queue.put((success, latency, error))
                stub = await conn.acquire_stub()
                success, latency, error, should_reconnect = await self.make_request(stub)
                if should_reconnect:
                    await conn.report_failure()
                await progress_queue.put((success, latency, error))

        if requests_to_make is None:
            async def infinite_request_loop():
                while True:
                    if stop_event is not None and stop_event.is_set():
                        return
                    await bounded_request()

            await asyncio.gather(
                *[infinite_request_loop() for _ in range(self.config.requests_per_connection)]
            )
            return

        await asyncio.gather(*[bounded_request() for _ in range(requests_to_make)])

    async def progress_tracker(
        self,
        progress_queue: asyncio.Queue,
        total_requests: Optional[int],
        stop_event: Optional[Event] = None,
    ):
        completed = 0
        last_print_time = time.time()

        while True:
            if total_requests is not None and completed >= total_requests:
                break
            if total_requests is None and stop_event is not None and stop_event.is_set() and progress_queue.empty():
                break

            try:
                success, latency, error = await asyncio.wait_for(progress_queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                success = None
                latency = None
                error = ""

            if success is not None and latency is not None:
                self.metrics.record_request(
                    success,
                    latency,
                    error,
                    time.time(),
                    self.config.parallel_connections * self.config.requests_per_connection,
                )

                completed += 1

            current_time = time.time()
            should_print = current_time - last_print_time >= 0.5
            if total_requests is not None:
                should_print = should_print or completed == total_requests

            if should_print:
                elapsed = current_time - self.metrics.start_time
                rate = completed / elapsed if elapsed > 0 else 0
                if total_requests is None:
                    sys.stdout.write(
                        f"\r[running] {completed} requests "
                        f"| {rate:.1f} req/s | Success: {self.metrics.successful_requests} "
                        f"| Failed: {self.metrics.failed_requests}    "
                    )
                else:
                    progress_pct = (completed / total_requests) * 100
                    sys.stdout.write(
                        f"\r[{progress_pct:5.1f}%] {completed}/{total_requests} requests "
                        f"| {rate:.1f} req/s | Success: {self.metrics.successful_requests} "
                        f"| Failed: {self.metrics.failed_requests}    "
                    )
                sys.stdout.flush()
                last_print_time = current_time

        print()

    async def run_stress_test(self, cancel_event: Optional[Event] = None) -> TestMetrics:
        infinite_mode = self.config.total_requests == -1

        print(f"\n{'='*70}")
        print(f"{self.service_name} Stress Test")
        print(f"{'='*70}")
        print(f"Target:                  {self.config.host}")
        if infinite_mode:
            print("Total Requests:          infinite (-1)")
        else:
            print(f"Total Requests:          {self.config.total_requests}")
        print(f"Parallel Connections:    {self.config.parallel_connections}")
        print(f"Requests per Connection: {self.config.requests_per_connection}")
        print(f"Timeout:                 {self.config.timeout}s")
        log_status = "enabled" if self.config.log_responses else "disabled"
        print(f"Log Responses:          {log_status}")
        if self.config.log_responses:
            print(f"Max Logged Responses:   {self.config.max_logged_responses}")
        print(f"{'='*70}\n")

        self.metrics = TestMetrics()
        await self.setup_connections()

        progress_queue = asyncio.Queue()
        self.metrics.start_time = time.time()

        progress_task = asyncio.create_task(
            self.progress_tracker(
                progress_queue,
                None if infinite_mode else self.config.total_requests,
                stop_event=cancel_event if infinite_mode else None,
            )
        )

        if infinite_mode:
            if cancel_event is None:
                raise ValueError("cancel_event is required when total_requests is -1")
            worker_tasks = [
                asyncio.create_task(
                    self.run_connection_worker(
                        self.connections[i],
                        None,
                        progress_queue,
                        stop_event=cancel_event,
                    )
                )
                for i in range(self.config.parallel_connections)
            ]
        else:
            requests_per_conn = self.config.total_requests // self.config.parallel_connections
            remainder = self.config.total_requests % self.config.parallel_connections
            worker_tasks = [
                asyncio.create_task(
                    self.run_connection_worker(
                        self.connections[i],
                        requests_per_conn + (1 if i < remainder else 0),
                        progress_queue,
                    )
                )
                for i in range(self.config.parallel_connections)
            ]

        try:
            await asyncio.gather(*worker_tasks)
            await progress_task
        finally:
            if not progress_task.done():
                progress_task.cancel()
                await asyncio.gather(progress_task, return_exceptions=True)
            self.metrics.end_time = time.time()
            self.metrics.finalize_windows(
                self.metrics.end_time,
                self.config.parallel_connections * self.config.requests_per_connection,
            )
            await self.teardown_connections()

        if self.config.window_metrics_csv:
            self.write_window_metrics_csv(self.config.window_metrics_csv)

        self.print_results()
        return self.metrics

    def write_window_metrics_csv(self, output_path: str):
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Timestamp",
                "User Count",
                "Type",
                "Name",
                "Requests/s",
                "Failures/s",
                "50%",
                "95%",
                "99%",
                "Total Request Count",
                "Total Failure Count",
                "Total Median Response Time",
                "Total Average Response Time",
            ])

            for m in self.metrics.window_metrics:
                writer.writerow([
                    m.timestamp,
                    m.user_count,
                    "",
                    "Aggregated",
                    f"{m.requests_per_second:.6f}",
                    f"{m.failures_per_second:.6f}",
                    f"{m.p50_latency_ms:.6f}",
                    f"{m.p95_latency_ms:.6f}",
                    f"{m.p99_latency_ms:.6f}",
                    m.total_request_count,
                    m.total_failure_count,
                    f"{m.total_median_response_time_ms:.6f}",
                    f"{m.total_average_response_time_ms:.6f}",
                ])

        print(f"Window metrics CSV written to {output_path} ({len(self.metrics.window_metrics)} rows)")

    def print_results(self):
        print(f"\n{'='*70}")
        print(f"Test Results")
        print(f"{'='*70}")
        print(f"Duration:             {self.metrics.duration:.2f}s")
        print(f"Total Requests:       {self.metrics.total_requests}")
        print(f"Successful:           {self.metrics.successful_requests}")
        print(f"Failed:               {self.metrics.failed_requests}")
        print(f"Success Rate:         {self.metrics.success_rate:.2f}%")
        print(f"\nThroughput:")
        print(f"  Requests/sec:       {self.metrics.requests_per_second:.2f}")
        print(f"\nLatency (ms):")
        print(f"  Min:                {self.metrics.min_latency * 1000:.2f}")
        print(f"  Average:            {self.metrics.avg_latency * 1000:.2f}")
        print(f"  P50:                {self.metrics.p50_latency * 1000:.2f}")
        print(f"  P95:                {self.metrics.p95_latency * 1000:.2f}")
        print(f"  P99:                {self.metrics.p99_latency * 1000:.2f}")
        print(f"  Max:                {self.metrics.max_latency * 1000:.2f}")

        if self.config.log_responses and self.metrics.sample_responses:
            print(f"\nSample responses (showing up to {self.config.max_logged_responses}):")
            for idx, resp in enumerate(self.metrics.sample_responses, start=1):
                print(f"  [{idx}] {resp}")

        if self.metrics.errors:
            print(f"\nErrors:")
            for error, count in sorted(self.metrics.errors.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  [{count:4d}x] {error[:100]}")

        if self.metrics.window_metrics:
            print(f"\nWindowed Metrics:     {len(self.metrics.window_metrics)} one-second snapshots")

        print(f"{'='*70}\n")

class ShippingServiceStressTester(StressTester):
    """Stress tester for ShippingService (GetQuote / ShipOrder)."""

    service_name = "ShippingService"

    def create_stub(self, channel):
        return demo_pb2_grpc.ShippingServiceStub(channel)

    async def call_rpc(self, stub):
        address = self._sample_address()
        items = self._sample_cart_items()
        if self.config.rpc_method == "GetQuote":
            return await stub.GetQuote(
                demo_pb2.GetQuoteRequest(address=address, items=items), # pyright: ignore[reportAttributeAccessIssue]
                timeout=self.config.timeout,
            )
        return await stub.ShipOrder(
            demo_pb2.ShipOrderRequest(address=address, items=items), # pyright: ignore[reportAttributeAccessIssue]
            timeout=self.config.timeout,
        )

    def _sample_address(self) -> demo_pb2.Address: # pyright: ignore[reportAttributeAccessIssue]
        addresses = [
            ("1600 Amphitheatre Parkway", "Mountain View", "CA", "USA", 94043),
            ("1 Infinite Loop", "Cupertino", "CA", "USA", 95014),
            ("410 Terry Ave North", "Seattle", "WA", "USA", 98109),
            ("1 Microsoft Way", "Redmond", "WA", "USA", 98052),
            ("1 Apple Park Way", "Cupertino", "CA", "USA", 95014),
        ]
        addr = random.choice(addresses)
        return demo_pb2.Address( # pyright: ignore[reportAttributeAccessIssue]
            street_address=addr[0], city=addr[1], state=addr[2],
            country=addr[3], zip_code=addr[4]
        )

    def _sample_cart_items(self) -> List[demo_pb2.CartItem]: # pyright: ignore[reportAttributeAccessIssue]
        return [
            demo_pb2.CartItem(product_id=random.choice(PRODUCTS), quantity=random.randint(1, 10)) # pyright: ignore[reportAttributeAccessIssue]
            for _ in range(random.randint(1, 5))
        ]


class SyntheticServiceStressTester(StressTester):
    """Stress tester for SyntheticService (RunWorkload)."""

    service_name = "SyntheticService"

    def create_stub(self, channel):
        return demo_pb2_grpc.SyntheticServiceStub(channel)

    async def call_rpc(self, stub):
        return await stub.RunWorkload(
            demo_pb2.WorkloadRequest(), # pyright: ignore[reportAttributeAccessIssue]
            timeout=self.config.timeout,
        )


class CartServiceStressTester(StressTester):
    """Stress tester for CartService (AddItem / GetCart / EmptyCart)."""

    service_name = "CartService"

    _USER_IDS = [f"stress-user-{i}" for i in range(20)]

    def create_stub(self, channel):
        return demo_pb2_grpc.CartServiceStub(channel)

    async def call_rpc(self, stub):
        user_id = random.choice(self._USER_IDS)
        method = self.config.rpc_method
        if method == "GetCart":
            return await stub.GetCart(
                demo_pb2.GetCartRequest(user_id=user_id), # pyright: ignore[reportAttributeAccessIssue]
                timeout=self.config.timeout,
            )
        if method == "EmptyCart":
            return await stub.EmptyCart(
                demo_pb2.EmptyCartRequest(user_id=user_id), # pyright: ignore[reportAttributeAccessIssue]
                timeout=self.config.timeout,
            )
        # Default: AddItem
        return await stub.AddItem(
            demo_pb2.AddItemRequest( # pyright: ignore[reportAttributeAccessIssue]
                user_id=user_id,
                item=demo_pb2.CartItem( # pyright: ignore[reportAttributeAccessIssue]
                    product_id=random.choice(PRODUCTS),
                    quantity=random.randint(1, 10),
                ),
            ),
            timeout=self.config.timeout,
        )


class RecommendationServiceStressTester(StressTester):
    """Stress tester for RecommendationService (ListRecommendations)."""

    service_name = "RecommendationService"

    _USER_IDS = [f"stress-user-{i}" for i in range(20)]

    def create_stub(self, channel):
        return demo_pb2_grpc.RecommendationServiceStub(channel)

    async def call_rpc(self, stub):
        # Send a random subset of products as the "currently viewed" context
        num_products = random.randint(1, 4)
        product_ids = random.sample(PRODUCTS, num_products)
        return await stub.ListRecommendations(
            demo_pb2.ListRecommendationsRequest( # pyright: ignore[reportAttributeAccessIssue]
                user_id=random.choice(self._USER_IDS),
                product_ids=product_ids,
            ),
            timeout=self.config.timeout,
        )
from sma import Triggerable
class SMAStressTester(Triggerable):
    def __init__(self, cnf: StressTestConfig, tester: StressTester) -> None:
        self.config = cnf
        self.tester = tester

    def trigger(self, cancel: Event, **kwargs) -> Dict[str, Any] | None:
        if self.config.total_requests < -1:
            raise ValueError("total_requests must be -1 (infinite mode) or >= 0")
        if self.config.total_requests == 0:
            print("Total requests is set to 0, skipping stress test.")
            return None
        async def run() -> Dict[str, Any] | None:
            work_task = asyncio.create_task(self.tester.run_stress_test(cancel_event=cancel))

            # Wait for either completion of the work or a cancel signal, without
            # calling into the loop from a different thread.
            cancel_task = asyncio.create_task(asyncio.to_thread(cancel.wait))
            
            try:
                done, _ = await asyncio.wait(
                    {work_task, cancel_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # cancel is already set - workers check stop_event.is_set() and
                # will exit gracefully after their in-flight request finishes.
                # Do NOT cancel work_task: that throws CancelledError into
                # run_stress_test's finally block and triggers a hang in
                # asyncio.wait_for inside progress_tracker (Python < 3.12 bug).
                #
                # Safety-net: each in-flight request is bounded by the per-request
                # gRPC timeout (self.config.timeout), so workers will drain within
                # that window. We add a small fixed buffer for scheduling overhead.
                drain_timeout = self.config.timeout + 5
                try:
                    results = await asyncio.wait_for(
                        work_task,
                        timeout=drain_timeout,
                    )
                    return results.to_dict()
                except asyncio.TimeoutError:
                    work_task.cancel()
                    await asyncio.gather(work_task, return_exceptions=True)
                    return None
            except asyncio.CancelledError:
                return None
            finally:
                if not cancel_task.done():
                    cancel_task.cancel()

        return asyncio.run(run())
    
    @property
    def metrics(self) -> TestMetrics:
        return self.tester.metrics
    
    def write_metrics(self, data_dir: str, meta_data_filename: str = "loadtest_metrics.json", window_metrics_csv: str = "loadtest_window_metrics.csv") -> str:
        self.tester.write_window_metrics_csv(f"{data_dir}/{window_metrics_csv}")
        with open(f"{data_dir}/{meta_data_filename}", "w", encoding="utf-8") as f:
            import json
            metrics_dict = self.tester.metrics.to_dict()
            del metrics_dict["window_metrics"]  # Window metrics are written separately to CSV
            json.dump(metrics_dict, f, indent=2)
        return f"{data_dir}/{meta_data_filename}"
    

    @staticmethod
    def make_tester(service_name: str, config: StressTestConfig) -> "SMAStressTester":
        if service_name == 'shipping':
            tester = ShippingServiceStressTester(config)
        elif service_name == 'cart':
            tester = CartServiceStressTester(config)
        elif service_name == 'recommendation':
            tester = RecommendationServiceStressTester(config)
        else:
            tester = SyntheticServiceStressTester(config)
            
        return SMAStressTester(config, tester)


def main():
    parser = argparse.ArgumentParser(
        description="Stress test tool for gRPC services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stress test SyntheticService (default)
  python stress_test.py -n 1000

  # Stress test ShippingService
  python stress_test.py --service shipping -n 1000 -m GetQuote

  # High concurrency test
  python stress_test.py -n 10000 -c 50 -r 20

    # Run continuously until cancelled
    python stress_test.py -n -1

  # Test with custom host
  python stress_test.py -n 1000 --host syntheticservice:50051
        """
    )

    parser.add_argument('--host', '-H', default='localhost:50051',
                        help='Service host address (default: localhost:50051)')
    parser.add_argument('--service', '-s',
                        choices=['shipping', 'synthetic', 'cart', 'recommendation'],
                        default='synthetic',
                        help='Service to test (default: synthetic)')
    parser.add_argument('--total-requests', '-n', type=int, default=1000,
                        help='Total number of requests; use -1 for continuous mode (default: 1000)')
    parser.add_argument('--parallel-connections', '-c', type=int, default=10,
                        help='Number of parallel gRPC connections (default: 10)')
    parser.add_argument('--requests-per-connection', '-r', type=int, default=10,
                        help='Max concurrent requests per connection (default: 10)')
    parser.add_argument('--method', '-m',
                        choices=['GetQuote', 'ShipOrder', 'AddItem', 'GetCart', 'EmptyCart'],
                        default='GetQuote',
                        help='RPC method (shipping: GetQuote/ShipOrder; cart: AddItem/GetCart/EmptyCart)')
    parser.add_argument('--timeout', '-t', type=float, default=5.0,
                        help='Request timeout in seconds (default: 5.0)')
    parser.add_argument('--log-responses', action='store_true',
                        help='Capture sample responses for validation')
    parser.add_argument('--max-logged-responses', type=int, default=5,
                        help='Maximum number of sample responses to keep')
    parser.add_argument('--window-metrics-csv', default=None,
                        help='Optional CSV path to write per-second windowed metrics')
    parser.add_argument('--recycle-after-requests', type=int, default=100,
                        help='Recycle each channel after N requests so newly scaled pods get '
                             'traffic; 0 disables (default: 100)')
    parser.add_argument('--no-reconnect-on-error', action='store_true',
                        help='Do not recycle the channel on transport-level failures')
    parser.add_argument('--error-backoff', type=float, default=0.1,
                        help='Pause in seconds after a failed request (default: 0.1)')

    args = parser.parse_args()

    config = StressTestConfig(
        host=args.host,
        total_requests=args.total_requests,
        parallel_connections=args.parallel_connections,
        requests_per_connection=args.requests_per_connection,
        rpc_method=args.method,
        timeout=args.timeout,
        log_responses=args.log_responses,
        max_logged_responses=args.max_logged_responses,
        window_metrics_csv=args.window_metrics_csv,
        recycle_after_requests=args.recycle_after_requests,
        reconnect_on_error=not args.no_reconnect_on_error,
        error_backoff=args.error_backoff
    )

    if config.total_requests != -1 and config.total_requests < config.parallel_connections:
        print(f"Error: total_requests ({config.total_requests}) must be >= parallel_connections ({config.parallel_connections})")
        sys.exit(1)

    runner = SMAStressTester.make_tester(args.service, config)
    
    cancel_event = Event()
    
    try:
        runner.trigger(cancel_event)
    except KeyboardInterrupt:
        cancel_event.set()
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
