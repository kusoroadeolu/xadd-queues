package io.github.kusoroadeolu.xaddq;

import org.jetbrains.lincheck.Lincheck;
import org.jetbrains.lincheck.datastructures.ModelCheckingOptions;
import org.jetbrains.lincheck.datastructures.Operation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LincheckCRQTest {
    private static final Xadd.Kind k = Xadd.Kind.XADD;
    public LPRQueue.CRQ<Integer> q = new LPRQueue.CRQ<>(2, k);

    @Operation
    public void dequeue() {
        q.dequeue();
    }

    @Operation
    public void enqueue() {
        q.enqueue(1);
    }

    @Test
    public void linearizableEnqueueDequeue() {
        new ModelCheckingOptions().check(this.getClass());
    }

    @Test
    void ensureNoLostWrites() {
        Lincheck.runConcurrentTest(() -> {
            LPRQueue.CRQ<Integer> q = new LPRQueue.CRQ<>(2, k);
            q.enqueue(1);
            q.enqueue(2);
            Runnable r = () -> {
                Integer i = q.dequeue();
                Assertions.assertNotNull(i);
            };

            Thread t = new Thread(r);
            Thread t1 = new Thread(r);
            t.start();
            t1.start();

            try {
                t.join();
                t.join();
            }catch (InterruptedException _){}
        });
    }


    //Tests fifo ordering and stress tests the empty transition in my implementation
    @Test
    void ensureFifoDequeues() {
        Lincheck.runConcurrentTest(() -> {
            LPRQueue.CRQ<Integer> q = new LPRQueue.CRQ<>(2, k);


            Runnable r = () -> {
                q.enqueue(1);
                q.enqueue(2);
            };

            Thread t = new Thread(q::dequeue);
            Thread t1 = new Thread(r);
            t.start();
            t1.start();

            try {
                t.join();
                t1.join();
            }catch (InterruptedException _){}

            Integer i = q.dequeue();
            Integer ii = q.dequeue();
            Assertions.assertTrue(i == 1 || i == 2);

            if (i == 1) Assertions.assertEquals(2, ii);
            else Assertions.assertNull(ii);
        });
    }
}