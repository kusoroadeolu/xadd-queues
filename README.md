# Xadd Queues
This project implements two concurrent unbounded fifo queues:
- [LPRQueue.java](https://nikitakoval.org/publications/ppopp23-lprq.pdf)
- [FAAArrayQueue.java](https://concurrencyfreaks.blogspot.com/2016/11/faaarrayqueue-mpmc-lock-free-queue-part.html)

which mainly use `fetchAndIncrement()` to obtain indices to enqueue and dequeue elements to/from.

The major goal of this project is mainly to benchmark the performance of my [AggregatingFetchAndAdd](https://github.com/kusoroadeolu/aggregating-funnel-counters)
against the provided fetchAndAdd cpu instruction when they are implemented in real datastructures (fifo queues in this case)

The implementations of these papers are in the `xadd-queue-core` module.

### Benchmark setup
- **Threads:** 8
- **JMH:** 3 warmup iterations, 7 measurement iterations, 3 forks
- **Mode:** Throughput in ops/µs (higher is better)

### Environment
There are some benchmark numbers in the `ProducerConsumerBench` class. These numbers were gotten from this environment.

**Note** These numbers can differ across different machines due to multiple factors.

| |                        |
|---|------------------------|
| CPU | Intel i5               |
| Cores / threads | 4 cores - 8 processors |
| RAM | 16GB                   |
| OS | Windows 11             |
| JDK | 25 (Open JDK)          |
| GC / heap flags | -Xms8g, -Xmx8g, -XX:+UseG1GC       |

## Running the benchmarks
To run the benchmarks for yourself you need to have JDK 25 and Maven installed. 

```bash
mvn clean package
cd xadd-queue-jmh
java -jar target/benchmark.jar
```

**NOTE:** If you're running these benchmarks on a computer with a different cpu count, I'd suggest you tweak the thread counts to match the number of cores present in your PC before running.