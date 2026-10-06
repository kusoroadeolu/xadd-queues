package io.github.kusoroadeolu.xaddq.jmh;

import io.github.kusoroadeolu.xaddq.ConcurrentQueue;
import io.github.kusoroadeolu.xaddq.FAAArrayQueue;
import io.github.kusoroadeolu.xaddq.LPRQueue;
import io.github.kusoroadeolu.xaddq.Xadd;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.github.kusoroadeolu.xaddq.jmh.JvmArgs.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 7, time = 1)
@Fork(value = 3, jvmArgs = {I_HEAP_ARG, M_HEAP_ARG, GC_TYPE_ARG})
public class ProducerConsumerBench {


    static final Object PAYLOAD = new Object();


    @State(Scope.Benchmark)
    public static class QueueState {
        @Param({"FAAQueue", "LRPQueue"})
        public String queueType;

        @Param({"Xadd", "AggregatingXadd"})
        public String xaddType;

        @Param({"100"})
        public long additionalWork;

        public ConcurrentQueue<Object> queue;

        @Setup(Level.Trial)
        public void setup() {
            queue = newQueue(queueType, xaddType);
        }

        @Setup(Level.Iteration)
        public void teardown() {
            while (queue.dequeue() != null);
        }

      static ConcurrentQueue<Object> newQueue(String type, String xaddType) {
            Xadd.Kind k = xaddType.equalsIgnoreCase("xadd") ? Xadd.Kind.XADD : Xadd.Kind.AGG_XADD;
            switch (type) {
                case "FAAQueue": return new FAAArrayQueue<>(k);
                case "LRPQueue" : return new LPRQueue<>(k);
                default: throw new IllegalArgumentException("Unknown queue type: " + type);
            }
        }
    }

    static void randomWork(long avg) {
        if (avg <= 0) return;
        Blackhole.consumeCPU(ThreadLocalRandom.current().nextLong(2 * avg + 1));
    }


    @Benchmark
    @OperationsPerInvocation(2)
    @Threads(8)
    public void enqDeqPairs(QueueState s, Blackhole bh) {
        s.queue.enqueue(PAYLOAD);
        randomWork(s.additionalWork);
        Object o = s.queue.dequeue();
        bh.consume(o);
        randomWork(s.additionalWork);
    }


    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    public static class ConsumerCounters {
        public long successfulDeq;
        public long queueEmpty;

        @Setup(Level.Iteration)
        public void reset() {
            successfulDeq = 0;
            queueEmpty = 0;
        }
    }

    @Benchmark
    @Group("producerConsumer")
    @GroupThreads(4)
    public void producer(QueueState s) {
        s.queue.enqueue(PAYLOAD);
        randomWork(s.additionalWork);
    }

    @Benchmark
    @Group("producerConsumer")
    @GroupThreads(4)
    public void consumer(QueueState s, ConsumerCounters c, Blackhole bh) {
        Object o = s.queue.dequeue();
        if (o != null) {
            c.successfulDeq++;
            bh.consume(o);
        } else {
            c.queueEmpty++;
        }
        randomWork(s.additionalWork);
    }
}

/* Normal Xadd
╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.enqDeqPairs ─╮
│  AdditionalWork QueueType XaddType        Score  Error   Unit       │
│  -------------- --------- --------------- ------ ------- ------     │
│  100            FAAQueue  Xadd            20.184 ± 0.276 ops/us     │
│  100            FAAQueue  AggregatingXadd 18.062 ± 0.443 ops/us     │
│  100            LRPQueue  Xadd            19.563 ± 0.623 ops/us     │
│  100            LRPQueue  AggregatingXadd 17.606 ± 0.667 ops/us     │
╰─────────────────────────────────────────────────────────────────────╯

╭─── io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.producerConsumer ────╮
│  AdditionalWork QueueType XaddType        Role          Score  Error   Unit    │
│  -------------- --------- --------------- ------------- ------ ------- ------  │
│  100            FAAQueue  Xadd            consumer      10.862 ± 0.306 ops/us  │
│  100            FAAQueue  Xadd            producer      10.889 ± 0.430 ops/us  │
│  100            FAAQueue  Xadd            queueEmpty    0.106  ± 0.171 ops/us  │
│  100            FAAQueue  Xadd            successfulDeq 10.795 ± 0.426 ops/us  │
│  100            FAAQueue  Xadd            aggregate     21.751 ± 0.704 ops/us  │
│  100            FAAQueue  AggregatingXadd consumer      10.049 ± 0.251 ops/us  │
│  100            FAAQueue  AggregatingXadd producer      10.647 ± 0.402 ops/us  │
│  100            FAAQueue  AggregatingXadd queueEmpty    0.057  ± 0.119 ops/us  │
│  100            FAAQueue  AggregatingXadd successfulDeq 10.017 ± 0.291 ops/us  │
│  100            FAAQueue  AggregatingXadd aggregate     20.695 ± 0.614 ops/us  │
│  100            LRPQueue  Xadd            consumer      8.307  ± 0.176 ops/us  │
│  100            LRPQueue  Xadd            producer      7.669  ± 0.264 ops/us  │
│  100            LRPQueue  Xadd            queueEmpty    0.636  ± 0.109 ops/us  │
│  100            LRPQueue  Xadd            successfulDeq 7.697  ± 0.263 ops/us  │
│  100            LRPQueue  Xadd            aggregate     15.976 ± 0.437 ops/us  │
│  100            LRPQueue  AggregatingXadd consumer      7.993  ± 0.496 ops/us  │
│  100            LRPQueue  AggregatingXadd producer      7.225  ± 0.881 ops/us  │
│  100            LRPQueue  AggregatingXadd queueEmpty    0.769  ± 0.386 ops/us  │
│  100            LRPQueue  AggregatingXadd successfulDeq 7.276  ± 0.868 ops/us  │
│  100            LRPQueue  AggregatingXadd aggregate     15.218 ± 1.375 ops/us  │
╰────────────────────────────────────────────────────────────────────────────────╯
* */
