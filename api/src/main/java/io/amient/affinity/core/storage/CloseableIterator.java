package io.amient.affinity.core.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

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
