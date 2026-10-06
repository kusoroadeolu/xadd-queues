package io.github.kusoroadeolu.xaddq;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;


class MSQueueProducerLPad {
    byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
    byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
    byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
    byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
    byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
    byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
    byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
    byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
    byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
    byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
    byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
    byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
    byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
    byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
    byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
}

class MSQueueProducerField<E> extends MSQueueProducerLPad {
    volatile MSQueue.Node<E> producerNode;
}

class MSQueueProducerRPad<E> extends MSQueueProducerField<E> {
    byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
    byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
    byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
    byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
    byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
    byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
    byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
    byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
    byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
    byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
    byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
    byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
    byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
    byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
    byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
}

class MSQueueConsumerField<E> extends MSQueueProducerRPad<E> {
    volatile MSQueue.Node<E> consumerNode;
}

class MSQueueConsumerRPad<E> extends MSQueueConsumerField<E> {
    byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
    byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
    byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
    byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
    byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
    byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
    byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
    byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
    byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
    byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
    byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
    byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
    byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
    byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
    byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
}

/*
 * A faithfully unfaithful impl of the popular MS Queue. A more robust and well optimized version can be
 * found in the java.util.concurrent package
 *
 * This is mostly just a learning exercise to actually understanding the MS algo (as I plan to build other queues that depend on this algo)
 * rather than larping it, cause tbh, I've never built it, just read about it
 *
 * Paper: https://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf
 *  */
public class MSQueue<E> extends MSQueueConsumerRPad<E> implements ConcurrentQueue<E> {

    public MSQueue() {
        producerNode = consumerNode = newNode(null);
    }

    @Override
    public boolean enqueue(E e) {
        Node<E> newNode = newNode(e);
        for (;;) {
            var p = producerNode;
            var next = p.next;

            if (next == null) {
                if (NEXT.compareAndSet(p, null, newNode)) { //linearization point
                    PRODUCER_NODE.compareAndSet(this, p, newNode);
                    break;
                }

            } else {
                //volatile read isn't necessary here
                if (p == producerNode) PRODUCER_NODE.compareAndSet(this, p, next); //help cas producer node to next
            }
        }

        return true;
    }


    @Override
    public E dequeue() {
        for (;;) {
            var c = consumerNode;
            Node<E> next = c.next;

            if (next == null) return null;
            else if (next == c) continue;

            E item = next.item;
            if (CONSUMER_NODE.compareAndSet(this, c, next)) {
                c.next = c; //help gc with self linking
                return item;
            }
        }
    }

    static class Node<E> {
        final E item;
        volatile Node<E> next;

        public Node(E item) {
            this.item = item;
        }
    }

    static <E>Node<E> newNode(E e) {
        return new Node<>(e);
    }


    static final VarHandle PRODUCER_NODE;
    static final VarHandle CONSUMER_NODE;
    static final VarHandle NEXT;

    static {
        var l = MethodHandles.lookup();
        try {
            PRODUCER_NODE = l.findVarHandle(MSQueue.class, "producerNode", Node.class);
            CONSUMER_NODE = l.findVarHandle(MSQueue.class, "consumerNode", Node.class);
            NEXT = l.findVarHandle(Node.class, "next", Node.class);
        }catch (Exception e){
            throw new ExceptionInInitializerError(e);
        }
    }
}
