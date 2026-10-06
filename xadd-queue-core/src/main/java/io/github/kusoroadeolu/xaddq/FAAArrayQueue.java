package io.github.kusoroadeolu.xaddq;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

class FAAQueueProducerLPad {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;
}

class FAAQueueProducerField<E> extends FAAQueueProducerLPad {
    volatile FAAArrayQueue.Node<E> producerNode;
}

class FAAQueueProducerRPad<E> extends FAAQueueProducerField<E> {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;

}

class FAAQueueConsumerField<E> extends FAAQueueProducerRPad<E> {
    volatile FAAArrayQueue.Node<E> consumerNode;
}

class FAAQueueConsumerRPad<E> extends FAAQueueConsumerField<E> {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;

}

class NodePad0 {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;
}

class NodeEnqField extends NodePad0 {
    final Xadd enq;

    NodeEnqField(Xadd enq) {
        this.enq = enq;
    }
}

class NodePad1 extends NodeEnqField {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;

    NodePad1(Xadd enq) {
        super(enq);
    }
}

class NodeDeqField extends NodePad1 {
    final Xadd deq;

    NodeDeqField(Xadd enq, Xadd deq) {
        super(enq);
        this.deq = deq;
    }
}

class NodePad2 extends NodeDeqField{
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;


    NodePad2(Xadd enq, Xadd deq) {
        super(enq, deq);
    }
}

class NodeTailFields<E> extends NodePad2 {
    final E[] items;
    volatile FAAArrayQueue.Node<E> next;

    public NodeTailFields(Xadd enq, Xadd deq, E[] items) {
        super(enq, deq);
        this.items = items;
    }
}

class NodePad3<E> extends NodeTailFields<E> {
    long p01,p02,p03,p04,p05,p06,p07,p08,p09,p10,p11,p12,p13,p14,p15;


    public NodePad3(Xadd enq, Xadd deq, E[] items) {
        super(enq, deq, items);
    }
}

public class FAAArrayQueue<E> extends FAAQueueConsumerRPad<E> implements ConcurrentQueue<E> {

    static final int CAPACITY = 1024;

    final E consumed = (E) new Object();
    final Xadd.Kind kind;

    public FAAArrayQueue(Xadd.Kind kind) {
        this.kind = kind;
        consumerNode = producerNode = newNode();
    }

    @Override
    public boolean enqueue(E e) {
        var p = producerNode;

        for (;;) {
            int index = (int) p.enq.fetchAndIncrement();
            if (index >= CAPACITY) {
                var next = p.next;

                if (next == null) {
                    Node<E> newNode = newNode(e);
                    if (NEXT.compareAndSet(p, null, newNode)) {
                        PRODUCER_NODE.compareAndSet(this, p, newNode);
                        break;
                    }

                } else if (p == producerNode) {
                    PRODUCER_NODE.compareAndSet(this, p, next);
                    p = next;
                    continue;
                }

                p = producerNode;

            } else {
                E prev = (E) ITEMS.getAndSet(p.items, index, e);
                if (prev == null) break;
            }
        }

        return true;
    }

    @Override
    public E dequeue() {
        var c = consumerNode;

        if (c.deq.get() >= c.enq.get() && c.next == null) return null;

        for (;;) {
            var xadd = c.deq;

            int index = (int) xadd.fetchAndIncrement();
            if (index >= CAPACITY) {
                Node<E> next;
                if ((next = c.next) == null) return null;
                CONSUMER_NODE.compareAndSet(this, c, next);
                c = consumerNode;
            } else {
                E prev = (E) ITEMS.getAndSet(c.items, index, consumed);
                if (prev != null) return prev;
            }
        }
    }

    Node<E> newNode(E e) {
        return new Node<>(e, kind);
    }

    Node<E> newNode() {
        return new Node<>(kind);
    }

    static class Node<E> extends NodePad3<E> {


        public Node(E item, Xadd.Kind kind) {
            this(kind);
            ITEMS.set(items, 0, item);
            enq.compareAndSet(0, 1);
        }

        public Node(Xadd.Kind kind) {
            super(Xadd.ofKind(kind), Xadd.ofKind(kind), (E[]) new Object[CAPACITY]);
        }
    }


    static final VarHandle PRODUCER_NODE;
    static final VarHandle CONSUMER_NODE;
    static final VarHandle NEXT;
    static final VarHandle ITEMS;

    static {
        var l = MethodHandles.lookup();
        try {
            PRODUCER_NODE = l.findVarHandle(FAAArrayQueue.class, "producerNode", Node.class);
            CONSUMER_NODE = l.findVarHandle(FAAArrayQueue.class, "consumerNode", Node.class);
            NEXT = l.findVarHandle(Node.class, "next", Node.class);
            ITEMS = MethodHandles.arrayElementVarHandle(Object[].class);
        }catch (Exception e){
            throw new ExceptionInInitializerError(e);
        }
    }

}
