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
        @Param({"FAAQueue", "LRPQueue"})
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
                case "FAAQueue": return new FAAArrayQueue<>(Xadd.Kind.AGG_XADD);
                case "LRPQueue" : return new LPRQueue<>(Xadd.Kind.AGG_XADD);
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
│  100            FAAQueue  20.170 ± 0.370 ops/us                     │
│  100            LRPQueue  19.666 ± 0.407 ops/us                     │
╰─────────────────────────────────────────────────────────────────────╯

╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.producerConsumer ─╮
│  AdditionalWork QueueType Role          Score  Error   Unit              │
│  -------------- --------- ------------- ------ ------- ------            │
│  100            FAAQueue  consumer      10.946 ± 0.278 ops/us            │
│  100            FAAQueue  producer      10.994 ± 0.258 ops/us            │
│  100            FAAQueue  queueEmpty    0.040  ± 0.040 ops/us            │
│  100            FAAQueue  successfulDeq 10.948 ± 0.264 ops/us            │
│  100            FAAQueue  aggregate     21.940 ± 0.524 ops/us            │
│  100            LRPQueue  consumer      8.316  ± 0.192 ops/us            │
│  100            LRPQueue  producer      7.660  ± 0.372 ops/us            │
│  100            LRPQueue  queueEmpty    0.655  ± 0.195 ops/us            │
│  100            LRPQueue  successfulDeq 7.707  ± 0.383 ops/us            │
│  100            LRPQueue  aggregate     15.977 ± 0.560 ops/us            │
╰──────────────────────────────────────────────────────────────────────────╯

* */

/* Aggregating Xadd
╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.enqDeqPairs ─╮
│  AdditionalWork QueueType Score  Error   Unit                       │
│  -------------- --------- ------ ------- ------                     │
│  100            FAAQueue  17.568 ± 0.503 ops/us                     │
│  100            LRPQueue  17.526 ± 0.684 ops/us                     │
╰─────────────────────────────────────────────────────────────────────╯

╭ io.github.kusoroadeolu.xaddq.jmh.ProducerConsumerBench.producerConsumer ─╮
│  AdditionalWork QueueType Role          Score  Error   Unit              │
│  -------------- --------- ------------- ------ ------- ------            │
│  100            FAAQueue  consumer      9.935  ± 0.323 ops/us            │
│  100            FAAQueue  producer      10.428 ± 0.500 ops/us            │
│  100            FAAQueue  queueEmpty    0.069  ± 0.196 ops/us            │
│  100            FAAQueue  successfulDeq 9.875  ± 0.420 ops/us            │
│  100            FAAQueue  aggregate     20.363 ± 0.773 ops/us            │
│  100            LRPQueue  consumer      7.956  ± 0.234 ops/us            │
│  100            LRPQueue  producer      7.265  ± 0.403 ops/us            │
│  100            LRPQueue  queueEmpty    0.687  ± 0.194 ops/us            │
│  100            LRPQueue  successfulDeq 7.327  ± 0.405 ops/us            │
│  100            LRPQueue  aggregate     15.221 ± 0.631 ops/us            │
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