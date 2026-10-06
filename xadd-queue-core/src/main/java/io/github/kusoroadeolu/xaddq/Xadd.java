package io.github.kusoroadeolu.xaddq;

import io.github.kusoroadeolu.afc.AggregatingXadd;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public interface Xadd {
        long fetchAndIncrement();
        long get();
        boolean compareAndSet(long t, long h);

    public  enum Kind {
        XADD, AGG_XADD
        }

         static Xadd ofKind(Kind kind) {
            return switch (kind) {
                case AGG_XADD -> new AggXadd();
                case XADD -> new DefaultXadd();
            };
        }

    static class AggXadd implements Xadd {
        final AggregatingXadd xadd = new AggregatingXadd(true);

        @Override
        public long fetchAndIncrement() {
            return xadd.fetchAndIncrement();
        }

        @Override
        public long get() {
            return xadd.get();
        }

        @Override
        public boolean compareAndSet(long t, long h) {
            return xadd.compareAndSet(t, h);
        }
    }


    static class DefaultXadd implements Xadd {
        volatile long value;


        public long fetchAndIncrement() {
            return (long) VALUE.getAndAdd(this, 1);
        }

        @Override
        public long get() {
            return value;
        }

        @Override
        public boolean compareAndSet(long t, long h) {
            return VALUE.compareAndSet(this, t, h);
        }

        static final VarHandle VALUE;

        static {
            var l = MethodHandles.lookup();
            try {
                VALUE = l.findVarHandle(DefaultXadd.class, "value", long.class);
            }catch (Exception e){
                throw new ExceptionInInitializerError(e);
            }
        }


    }
}