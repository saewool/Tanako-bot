"""
ColumnarDB Benchmark Script
Test actual performance on current environment
"""

import asyncio
import time
import sys
import os
import psutil
import statistics

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.database.engine import ColumnarDB, Column
from src.database.storage import DataType
from src.database.query import query


class Benchmark:
    def __init__(self):
        self.db = ColumnarDB("data/benchmark_db")
        self.results = {}
    
    async def setup(self):
        """Create test table"""
        columns = [
            Column("id", DataType.INT64, primary_key=True, auto_increment=True),
            Column("guild_id", DataType.INT64, indexed=True),
            Column("user_id", DataType.INT64, indexed=True),
            Column("username", DataType.STRING),
            Column("xp", DataType.INT32),
            Column("level", DataType.INT32),
            Column("messages", DataType.INT32),
            Column("data", DataType.JSON),
        ]
        await self.db.create_table("benchmark_users", columns, if_not_exists=True)
        print("✓ Test table created")
    
    async def benchmark_insert(self, count: int = 1000):
        """Benchmark INSERT operations"""
        print(f"\n📝 Testing INSERT ({count} records)...")
        
        times = []
        for i in range(count):
            start = time.perf_counter()
            await self.db.insert("benchmark_users", {
                "guild_id": 123456789 + (i % 10),
                "user_id": 100000000 + i,
                "username": f"user_{i}",
                "xp": i * 15,
                "level": i // 100,
                "messages": i * 5,
                "data": {"badges": ["member"], "joined": "2024-01-01"}
            })
            times.append(time.perf_counter() - start)
        
        total_time = sum(times)
        ops_per_sec = count / total_time
        avg_latency = statistics.mean(times) * 1000
        p99_latency = sorted(times)[int(count * 0.99)] * 1000
        
        self.results["insert"] = {
            "ops_per_sec": ops_per_sec,
            "avg_latency_ms": avg_latency,
            "p99_latency_ms": p99_latency,
            "total_time": total_time
        }
        
        print(f"   Ops/sec: {ops_per_sec:,.0f}")
        print(f"   Avg latency: {avg_latency:.2f}ms")
        print(f"   P99 latency: {p99_latency:.2f}ms")
    
    async def benchmark_select_by_id(self, count: int = 1000):
        """Benchmark SELECT by primary key"""
        print(f"\n🔍 Testing SELECT by ID ({count} queries)...")
        
        times = []
        for i in range(count):
            row_id = (i % 500) + 1
            start = time.perf_counter()
            q = query("benchmark_users").where("id", "=", row_id)
            await self.db.select("benchmark_users", condition=q)
            times.append(time.perf_counter() - start)
        
        total_time = sum(times)
        ops_per_sec = count / total_time
        avg_latency = statistics.mean(times) * 1000
        
        self.results["select_by_id"] = {
            "ops_per_sec": ops_per_sec,
            "avg_latency_ms": avg_latency,
            "total_time": total_time
        }
        
        print(f"   Ops/sec: {ops_per_sec:,.0f}")
        print(f"   Avg latency: {avg_latency:.2f}ms")
    
    async def benchmark_select_by_index(self, count: int = 500):
        """Benchmark SELECT by indexed column"""
        print(f"\n🔍 Testing SELECT by indexed column ({count} queries)...")
        
        times = []
        for i in range(count):
            guild_id = 123456789 + (i % 10)
            start = time.perf_counter()
            q = query("benchmark_users").where("guild_id", "=", guild_id)
            await self.db.select("benchmark_users", condition=q)
            times.append(time.perf_counter() - start)
        
        total_time = sum(times)
        ops_per_sec = count / total_time
        avg_latency = statistics.mean(times) * 1000
        
        self.results["select_by_index"] = {
            "ops_per_sec": ops_per_sec,
            "avg_latency_ms": avg_latency,
            "total_time": total_time
        }
        
        print(f"   Ops/sec: {ops_per_sec:,.0f}")
        print(f"   Avg latency: {avg_latency:.2f}ms")
    
    async def benchmark_select_all(self, count: int = 100):
        """Benchmark SELECT all (full table scan)"""
        print(f"\n🔍 Testing SELECT ALL ({count} queries)...")
        
        times = []
        for i in range(count):
            start = time.perf_counter()
            await self.db.select("benchmark_users", limit=100)
            times.append(time.perf_counter() - start)
        
        total_time = sum(times)
        ops_per_sec = count / total_time
        avg_latency = statistics.mean(times) * 1000
        
        self.results["select_all"] = {
            "ops_per_sec": ops_per_sec,
            "avg_latency_ms": avg_latency,
            "total_time": total_time
        }
        
        print(f"   Ops/sec: {ops_per_sec:,.0f}")
        print(f"   Avg latency: {avg_latency:.2f}ms")
    
    async def benchmark_update(self, count: int = 500):
        """Benchmark UPDATE operations"""
        print(f"\n✏️ Testing UPDATE ({count} operations)...")
        
        times = []
        for i in range(count):
            row_id = (i % 500) + 1
            start = time.perf_counter()
            q = query("benchmark_users").where("id", "=", row_id)
            await self.db.update(
                "benchmark_users",
                {"xp": i * 20, "messages": i * 10},
                condition=q
            )
            times.append(time.perf_counter() - start)
        
        total_time = sum(times)
        ops_per_sec = count / total_time
        avg_latency = statistics.mean(times) * 1000
        
        self.results["update"] = {
            "ops_per_sec": ops_per_sec,
            "avg_latency_ms": avg_latency,
            "total_time": total_time
        }
        
        print(f"   Ops/sec: {ops_per_sec:,.0f}")
        print(f"   Avg latency: {avg_latency:.2f}ms")
    
    async def benchmark_mixed_workload(self, count: int = 1000):
        """Benchmark mixed read/write (80% read, 20% write)"""
        print(f"\n🔄 Testing MIXED workload ({count} operations, 80% read)...")
        
        import random
        times = []
        
        for i in range(count):
            start = time.perf_counter()
            if random.random() < 0.8:
                row_id = random.randint(1, 500)
                q = query("benchmark_users").where("id", "=", row_id)
                await self.db.select("benchmark_users", condition=q)
            else:
                row_id = random.randint(1, 500)
                q = query("benchmark_users").where("id", "=", row_id)
                await self.db.update(
                    "benchmark_users",
                    {"xp": random.randint(0, 10000)},
                    condition=q
                )
            times.append(time.perf_counter() - start)
        
        total_time = sum(times)
        ops_per_sec = count / total_time
        avg_latency = statistics.mean(times) * 1000
        
        self.results["mixed"] = {
            "ops_per_sec": ops_per_sec,
            "avg_latency_ms": avg_latency,
            "total_time": total_time
        }
        
        print(f"   Ops/sec: {ops_per_sec:,.0f}")
        print(f"   Avg latency: {avg_latency:.2f}ms")
    
    async def cleanup(self):
        """Remove test data"""
        await self.db.drop_table("benchmark_users", if_exists=True)
        print("\n✓ Cleanup complete")
    
    def print_summary(self):
        """Print final summary"""
        print("\n" + "="*60)
        print("📊 BENCHMARK SUMMARY")
        print("="*60)
        
        process = psutil.Process()
        mem = process.memory_info()
        
        print(f"\n💻 System Info:")
        print(f"   CPU cores: {psutil.cpu_count()}")
        print(f"   RAM total: {psutil.virtual_memory().total / (1024**3):.1f} GB")
        print(f"   RAM used by test: {mem.rss / (1024**2):.1f} MB")
        
        print(f"\n📈 Results:")
        print(f"   {'Operation':<25} {'Ops/sec':>12} {'Avg Latency':>15}")
        print(f"   {'-'*25} {'-'*12} {'-'*15}")
        
        for op, data in self.results.items():
            print(f"   {op:<25} {data['ops_per_sec']:>12,.0f} {data['avg_latency_ms']:>12.2f} ms")
        
        print("\n📋 Discord Bot Capacity Estimate:")
        mixed_ops = self.results.get("mixed", {}).get("ops_per_sec", 1000)
        
        ops_per_message = 2
        messages_per_server_per_min = 10
        
        max_servers = int(mixed_ops * 60 / messages_per_server_per_min / ops_per_message * 0.5)
        
        print(f"   Mixed ops/sec: {mixed_ops:,.0f}")
        print(f"   Est. ~{ops_per_message} DB ops per message")
        print(f"   At 50% load capacity:")
        print(f"   → Safe for ~{max_servers} active servers")
        
        if max_servers < 50:
            print("\n   ⚠️ Performance is LIMITED. Consider:")
            print("      - Adding Redis cache layer")
            print("      - Switching to SQLite/PostgreSQL")
            print("      - This is expected for shared Replit environment")
        elif max_servers < 200:
            print("\n   ✅ Good for small-medium bots")
        else:
            print("\n   🚀 Excellent performance!")


async def main():
    print("="*60)
    print("🔥 ColumnarDB Benchmark")
    print("="*60)
    
    bench = Benchmark()
    
    try:
        await bench.setup()
        await bench.benchmark_insert(500)
        await bench.benchmark_select_by_id(500)
        await bench.benchmark_select_by_index(200)
        await bench.benchmark_select_all(50)
        await bench.benchmark_update(200)
        await bench.benchmark_mixed_workload(500)
        bench.print_summary()
    finally:
        #await bench.cleanup()
        pass


if __name__ == "__main__":
    asyncio.run(main())
