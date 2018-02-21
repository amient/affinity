package io.amient.affinity.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final public class CompletedJavaFuture<T> implements Future<T> {

    private final T result;

    public CompletedJavaFuture(T result) {
        this.result = result;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    public T get() throws ExecutionException, InterruptedException {
        return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return result;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }
}
