package io.github.kusoroadeolu.xaddq;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class LPRQLPad {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;
}

class LPRQConsumerCrqField<E> extends LPRQLPad {
    volatile LPRQueue.CRQ<E> consumerCrq;
}

class LPRQConsumerRPad<E> extends LPRQConsumerCrqField<E> {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;
}

class LPRQProducerCrq<E> extends LPRQConsumerRPad<E> {
    volatile LPRQueue.CRQ<E> producerCrq;
}

class LPRQProducerRPad<E> extends LPRQProducerCrq<E> {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;
}


class CRQPad0 {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;
}

class CRQProducerField extends CRQPad0 {
    Xadd producerXadd;
}

class CRQPad1 extends CRQProducerField {
    long q01,q02,q03,q04,q05,q06,q07,q08,q09,q10,q11,q12,q13,q14,q15;
}

class CRQConsumerField extends CRQPad1 {
    Xadd consumerXadd;
}

class CRQPad2 extends CRQConsumerField {
    long r01,r02,r03,r04,r05,r06,r07,r08,r09,r10,r11,r12,r13,r14,r15;
}

class CRQTailFields<E> extends CRQPad2 {
    LPRQueue.CRQ.Node[] items;
    volatile LPRQueue.CRQ<E> next;
    volatile boolean closed;
}

class CRQPad3<E> extends CRQTailFields<E> {
    long s01,s02,s03,s04,s05,s06,s07,s08,s09,s10,s11,s12,s13,s14,s15;
}

public class LPRQueue<E> extends LPRQProducerRPad<E> implements ConcurrentQueue<E>{


    final Xadd.Kind kind;

    public LPRQueue(Xadd.Kind kind) {
        this.kind = kind;
        producerCrq = consumerCrq = new CRQ<>(kind);
    }

    @Override
    public boolean enqueue(E e) {

        for (;;) {
            CRQ<E> producerCrq = this.producerCrq;
            CRQ<E> next;
            if (producerCrq.enqueue(e)) return true;
            else if ((next = producerCrq.next) != null) {
                PRODUCER_CRQ.compareAndSet(this, producerCrq, next);
            } else {
                CRQ<E> crq = new CRQ<>(kind, e);
                if (producerCrq.casNext(null, crq)) {
                    PRODUCER_CRQ.compareAndSet(this, producerCrq, crq);
                    return true;
                }
            }
        }
    }

    @Override
    public E dequeue() {
        for (;;) {
            CRQ<E> consumerCrq = this.consumerCrq;
            E res = consumerCrq.dequeue();
            if (res != null) return res;
            if (consumerCrq.next == null) return null;
            res = consumerCrq.dequeue();
            if (res != null) return res;
            CONSUMER_CRQ.compareAndSet(this, consumerCrq, consumerCrq.next);
        }
    }

    static final int CAPACITY = 1024;
    static final int MASK = CAPACITY - 1;


    public static class CRQ<E> extends CRQPad3<E> implements ConcurrentQueue<E> {

        public CRQ(Xadd.Kind kind) {
            this.items = new Node[CAPACITY];
            this.producerXadd = Xadd.ofKind(kind);
            this.consumerXadd = Xadd.ofKind(kind);
            producerXadd.compareAndSet(0, CAPACITY);
            consumerXadd.compareAndSet(0, CAPACITY);
            for (int i = 0; i < CAPACITY; ++i) {
                items[i] = new Node();
            }
        }

        public CRQ(Xadd.Kind kind, E e) {
            this.items = new Node[CAPACITY];
            this.producerXadd = Xadd.ofKind(kind);
            this.consumerXadd = Xadd.ofKind(kind);

            consumerXadd.compareAndSet(0, CAPACITY);

            for (int i = 0; i < CAPACITY; ++i) {
                items[i] = new Node();
            }

            items[0].item = e;
            items[0].safeAndEpoch = new SafeAndEpoch(true, 1);
            producerXadd.compareAndSet(0, CAPACITY + 1);
        }

        public boolean casNext(CRQ<E> from, CRQ<E> to) {
            return NEXT.compareAndSet(this, from, to);
        }

        public boolean enqueue(E e) {
            final Xadd producerXadd = this.producerXadd;
            final Thread token = Thread.currentThread();
            final Xadd  consumerXadd = this.consumerXadd;

            for (;;) {
                long producerIndex = producerXadd.fetchAndIncrement();
                if (closed) return false;

                long cycle = producerIndex / CAPACITY;
                int index = (int) (producerIndex & MASK);
                Node node = items[index];

                SafeAndEpoch se = node.safeAndEpoch;
                Object item = node.item;

                block: if ((item == null || isThreadToken(item))
                        && se.epoch() < cycle
                        && (se.safe() || consumerXadd.get() <= producerIndex)) {

                    if (!ITEM.compareAndSet(node, item, token)) break block; //a later enqueue has claimed this slot

                    if (!SAFE_AND_EPOCH.compareAndSet(node, se, new SafeAndEpoch(true, cycle))) { //a later enqueue has claimed this slot
                        // or a dequeue on this or a later cycle has advanced past this slot (i.e.) empty transition
                        ITEM.compareAndSet(node, token, null);
                        break block;
                    }

                    if (ITEM.compareAndSet(node, token, e)) return true;
                }

                if (producerIndex - consumerXadd.get() >= CAPACITY) { //is the queue full or?
                    if (!closed) closed = true;
                    return false;
                }

            }
        }

        public E dequeue() {
            final Xadd  consumerXadd = this.consumerXadd;
            final Xadd producerXadd = this.producerXadd;

            for (;;) {
                long consumerIndex = consumerXadd.fetchAndIncrement();

                long cycle = consumerIndex / CAPACITY;
                int index = (int) (consumerIndex & MASK);
                Node node = items[index];

                for (;;) {
                    SafeAndEpoch se = node.safeAndEpoch;
                    Object item = node.item;
                    SafeAndEpoch later = node.safeAndEpoch;


                    if (se != later) continue;

                    long epoch = se.epoch();
                    if (epoch == cycle && item != null && !isThreadToken(item)) {
                        node.item = null;
                        return (E) item;
                    } else if (epoch <= cycle && (item == null || isThreadToken(item))) {
                        if (isThreadToken(item) && !ITEM.compareAndSet(node, item, null)) continue;
                        if (SAFE_AND_EPOCH.compareAndSet(node, se, new SafeAndEpoch(se.safe(), cycle))) break;
                    } else if (epoch < cycle && !isThreadToken(item)) {
                        if(SAFE_AND_EPOCH.compareAndSet(node, se, new SafeAndEpoch(false, se.epoch()))) break;
                    } else break;

                }

                if (producerXadd.get() <= consumerIndex + 1){
                    fixState();
                    return null; //queue is empty
                }
            }

        }

        void fixState() {
            for (;;) {
                long t = producerXadd.get();
                long h = consumerXadd.get();
                if (producerXadd.get() != t) continue;
                if (!closed && h > t) {
                    if (producerXadd.compareAndSet(t, h)) break;
                    continue;
                }
                break;
            }
        }

        static boolean isThreadToken(Object o) {
            return o instanceof Thread;
        }

        static class Node {
            volatile Object item;
            volatile SafeAndEpoch safeAndEpoch;

            public Node() {
                SAFE_AND_EPOCH.setRelease(this, SafeAndEpoch.DEFAULT);
            }

            //ideally we could pack the safe bit (as an int) and epoch into a 64 bit long
            // but i couldnt be bothered, this is much simpler though at the cost of gc interference
        }

        record SafeAndEpoch(boolean safe, long epoch) {
            private static final SafeAndEpoch DEFAULT = new SafeAndEpoch(true, 0);
        }


        static final VarHandle ITEM;
        static final VarHandle SAFE_AND_EPOCH;
        public static final VarHandle NEXT;
        static {
            var l = MethodHandles.lookup();
            try {
                ITEM = l.findVarHandle(Node.class, "item", Object.class);
                SAFE_AND_EPOCH = l.findVarHandle(Node.class, "safeAndEpoch", SafeAndEpoch.class);
                NEXT = l.findVarHandle(CRQTailFields.class, "next", CRQ.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static final VarHandle PRODUCER_CRQ;
    static final VarHandle CONSUMER_CRQ;

    static {
        var l = MethodHandles.lookup();
        try {
            CONSUMER_CRQ = l.findVarHandle(LPRQueue.class, "consumerCrq", CRQ.class);
            PRODUCER_CRQ = l.findVarHandle(LPRQueue.class, "producerCrq", CRQ.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}