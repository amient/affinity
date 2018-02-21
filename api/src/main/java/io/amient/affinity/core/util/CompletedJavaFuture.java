package io.amient.affinity.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

final public class CompletedJavaFuture<T> implements Future<T> {

    private final Supplier<T> resultSupplier;

    public CompletedJavaFuture(Supplier<T> resultSupplier) {
        this.resultSupplier = resultSupplier;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    public T get() throws ExecutionException, InterruptedException {
        return resultSupplier.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return resultSupplier.get();
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
