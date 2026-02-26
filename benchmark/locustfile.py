"""
Load Test — Distributed Rate Limiter Service
---------------------------------------------
Run with:
    locust -f benchmark/locustfile.py --host=http://localhost:8000

Then open http://localhost:8089 to start the test.

Recommended test scenarios:
  1. Burst test:     100 users, spawn rate 100  → all spawn instantly
  2. Sustained test: 50 users,  spawn rate 5    → steady stream
  3. Mixed test:     200 users, spawn rate 10   → ramp up slowly
"""

from locust import HttpUser, task, between
import random
import time


CLIENT_IDS = [f"user_{i}" for i in range(1, 21)]   # 20 simulated clients
ENDPOINTS = ["/api/search", "/api/login", "/api/data", "/api/export"]


class RateLimiterUser(HttpUser):
    wait_time = between(0.05, 0.2)  # 50–200ms between requests per user

    def on_start(self):
        """Each simulated user picks a random client_id."""
        self.client_id = random.choice(CLIENT_IDS)
        self.endpoint = random.choice(ENDPOINTS)

    @task(4)
    def check_sliding_window(self):
        """Most traffic hits the /check endpoint (sliding window)."""
        start = time.perf_counter()
        with self.client.post(
            "/check",
            json={"client_id": self.client_id, "endpoint": self.endpoint},
            catch_response=True,
        ) as response:
            latency_ms = (time.perf_counter() - start) * 1000
            if response.status_code == 200:
                data = response.json()
                # Tag allowed vs denied for reporting
                label = "allowed" if data["allowed"] else "denied"
                response.success()
                # Log latency per outcome (visible in Locust stats)
                self.environment.events.request.fire(
                    request_type="POST",
                    name=f"/check [{label}]",
                    response_time=latency_ms,
                    response_length=len(response.content),
                )
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @task(1)
    def check_stats(self):
        """Occasional stats polling."""
        self.client.get(
            f"/stats/{self.client_id}",
            params={"endpoint": self.endpoint},
            name="/stats/[client_id]",
        )


class BurstUser(HttpUser):
    """
    Simulates a single client hammering the API to trigger rate limiting.
    Use this to measure how quickly limits kick in.
    """
    wait_time = between(0.001, 0.01)  # nearly no wait — pure burst
    weight = 1  # fewer burst users than normal users

    @task
    def burst_check(self):
        self.client.post(
            "/check",
            json={"client_id": "burst_tester", "endpoint": "/api/data"},
            name="/check [burst]",
        )
