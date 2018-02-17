package io.amient.affinity.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;

public class LogCheckpoint<C> {

    final public C position;

    final private static Logger log = LoggerFactory.getLogger(LogCheckpoint.class);

    public LogCheckpoint(C position) {
        this.position = position;
    }

    public LogCheckpoint() {
        this.position = null;
    }

    @Override
    public String toString() {
        return "Checkpoint(" + position + ")";
    }

    public Boolean isEmpty() {
        return position == null;
    }

    public static <C> LogCheckpoint<C> readFromFile(Path file) throws IOException {
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file.toFile()));
            try {
                return new LogCheckpoint<>((C)ois.readObject());
            } finally {
                ois.close();
            }
        } catch (Throwable e) {
            log.warn("Invalid checkpoint file: " + file + ", going to rewind fully.", e);
            return new LogCheckpoint<>();
        }
    }

    public void writeToFile(Path file) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file.toFile()));
        oos.writeObject(position);
        oos.close();
    }
}
