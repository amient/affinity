package io.amient.affinity.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

abstract public class MappedJavaFuture<A, T> implements Future<T> {

    final private Future<A> f;

    public MappedJavaFuture(Future<A> f) {
        this.f = f;
    }

    @Override
    public boolean isCancelled() {
        return f.isCancelled();
    }

    public abstract T map(A result);

    public T get() throws ExecutionException, InterruptedException {
        return map(f.get());
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return map(f.get(timeout, unit));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return f.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isDone() {
        return f.isDone();
    }
}
