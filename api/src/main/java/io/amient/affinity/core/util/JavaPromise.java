package io.amient.affinity.core.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JavaPromise<T> implements Future<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private T value;
    private Exception exception;

    public void failure(Exception e) {
        exception = e;
        latch.countDown();
    }

    public void success(T result) {
        value = result;
        latch.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    @Override
    public T get() throws InterruptedException {
        latch.await();
        if (exception != null) {
            throw new RuntimeException(exception);
        } else {
            return value;
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (latch.await(timeout, unit)) {
            return value;
        } else {
            throw new TimeoutException();
        }
    }

}