package io.github.kusoroadeolu.xaddq;

public interface ConcurrentQueue<E> {
    boolean enqueue(E e);
    E dequeue();
}
