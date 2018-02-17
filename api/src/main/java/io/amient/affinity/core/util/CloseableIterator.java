package io.amient.affinity.core.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    default long size() {
        long result = 0;
        while (hasNext()) {
            result++;
            next();
        }
        return result;
    }

    static <X> CloseableIterator<X> apply(Iterator<X> underlying) {
        return new CloseableIterator<X>() {
            @Override
            public void close() throws IOException {

            }

            @Override
            public boolean hasNext() {
                return underlying.hasNext();
            }

            @Override
            public X next() {
                return underlying.next();
            }
        };
    }
}
