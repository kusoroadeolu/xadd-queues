package io.github.kusoroadeolu.xaddq.jmh;

import io.github.kusoroadeolu.xaddq.ConcurrentQueue;
import io.github.kusoroadeolu.xaddq.FAAArrayQueue;
import io.github.kusoroadeolu.xaddq.LPRQueue;
import io.github.kusoroadeolu.xaddq.Xadd;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
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


    // Single shared payload, like the C++ version reusing one UserData
    static final Object PAYLOAD = new Object();

    static class JCToolsAdapter<E> implements ConcurrentQueue<E> {
        private final MpmcUnboundedXaddArrayQueue<E> queue;

        public JCToolsAdapter() {
            this.queue = new MpmcUnboundedXaddArrayQueue<>(1024, 0);
        }

        @Override
        public boolean enqueue(E e) {
            return queue.offer(e);
        }

        @Override
        public E dequeue() {
            return queue.poll();
        }
    }


    @State(Scope.Benchmark)
    public static class QueueState {
        @Param({"FAAQueue", "LRPQueue", "JcTools"})
        public String queueType;

        @Param({"100"})
        public long additionalWork;

        public ConcurrentQueue<Object> queue;

        @Setup(Level.Trial)
        public void setup() {
            queue = newQueue(queueType);
        }

        @Setup(Level.Iteration)
        public void teardown() {
            while (queue.dequeue() != null);
        }

      static ConcurrentQueue<Object> newQueue(String type) {
            switch (type) {
                case "JCTools": return new JCToolsAdapter<>();
                case "FAAQueue": return new FAAArrayQueue<>(Xadd.Kind.XADD);
                case "LRPQueue" : return new LPRQueue<>(Xadd.Kind.XADD);
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
│  AdditionalWork QueueType Score  Error   Unit                       │
│  -------------- --------- ------ ------- ------                     │
│  100            FAAQueue  20.197 ± 0.301 ops/us                     │
│  100            LRPQueue  20.100 ± 0.529 ops/us                     │
╰─────────────────────────────────────────────────────────────────────╯

╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.producerConsumer ─╮
│  AdditionalWork QueueType Role          Score  Error   Unit              │
│  -------------- --------- ------------- ------ ------- ------            │
│  100            FAAQueue  consumer      11.223 ± 0.276 ops/us            │
│  100            FAAQueue  producer      11.294 ± 0.335 ops/us            │
│  100            FAAQueue  queueEmpty    0.056  ± 0.083 ops/us            │
│  100            FAAQueue  successfulDeq 11.214 ± 0.315 ops/us            │
│  100            FAAQueue  aggregate     22.517 ± 0.599 ops/us            │
│  100            LRPQueue  consumer      8.688  ± 0.170 ops/us            │
│  100            LRPQueue  producer      8.000  ± 0.310 ops/us            │
│  100            LRPQueue  queueEmpty    0.690  ± 0.160 ops/us            │
│  100            LRPQueue  successfulDeq 8.029  ± 0.320 ops/us            │
│  100            LRPQueue  aggregate     16.689 ± 0.475 ops/us            │
╰──────────────────────────────────────────────────────────────────────────╯
* */

/* Aggregating Xadd
╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.enqDeqPairs ─╮
│  AdditionalWork QueueType Score  Error   Unit                       │
│  -------------- --------- ------ ------- ------                     │
│  100            FAAQueue  17.740 ± 0.760 ops/us                     │
│  100            LRPQueue  17.624 ± 0.404 ops/us                     │
╰─────────────────────────────────────────────────────────────────────╯

╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.producerConsumer ─╮
│  AdditionalWork QueueType Role          Score  Error   Unit              │
│  -------------- --------- ------------- ------ ------- ------            │
│  100            FAAQueue  consumer      10.036 ± 0.432 ops/us            │
│  100            FAAQueue  producer      11.037 ± 0.488 ops/us            │
│  100            FAAQueue  queueEmpty    0.028  ± 0.070 ops/us            │
│  100            FAAQueue  successfulDeq 10.053 ± 0.426 ops/us            │
│  100            FAAQueue  aggregate     21.073 ± 0.638 ops/us            │
│  100            LRPQueue  consumer      8.372  ± 0.399 ops/us            │
│  100            LRPQueue  producer      7.813  ± 0.640 ops/us            │
│  100            LRPQueue  queueEmpty    0.588  ± 0.256 ops/us            │
│  100            LRPQueue  successfulDeq 7.859  ± 0.647 ops/us            │
│  100            LRPQueue  aggregate     16.186 ± 1.032 ops/us            │
╰──────────────────────────────────────────────────────────────────────────╯
* */

/*
╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.enqDeqPairs ─╮
│  AdditionalWork QueueType Score  Error   Unit                       │
│  -------------- --------- ------ ------- ------                     │
│  100            JCTools   19.469 ± 1.202 ops/us                     │
╰─────────────────────────────────────────────────────────────────────╯

╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.producerConsumer ─╮
│  AdditionalWork QueueType Role          Score  Error   Unit              │
│  -------------- --------- ------------- ------ ------- ------            │
│  100            JCTools   consumer      11.166 ± 0.471 ops/us            │
│  100            JCTools   producer      14.412 ± 0.643 ops/us            │
│  100            JCTools   queueEmpty    0.000  ± 0.000 ops/us            │
│  100            JCTools   successfulDeq 11.198 ± 0.464 ops/us            │
│  100            JCTools   aggregate     25.578 ± 1.097 ops/us            │
╰──────────────────────────────────────────────────────────────────────────╯

* */