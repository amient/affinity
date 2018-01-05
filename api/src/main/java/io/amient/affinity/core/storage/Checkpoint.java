package io.amient.affinity.core.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class Checkpoint {

    final public long offset;

    final private static Logger log = LoggerFactory.getLogger(Checkpoint.class);

    public Checkpoint(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Checkpoint(offset: " + offset + ")";
    }

    public static Checkpoint readFromFile(Path file) throws IOException {
        java.util.List<String> lines = Files.readAllLines(file);
        long offset;
        try {
            offset = Long.valueOf(lines.get(0));
        } catch (Throwable e) {
            log.warn("Invalid checkpoint file: " + file + ", going to rewind fully.", e);
            offset = -1L;
        }
        return new Checkpoint(offset);
    }

    public void writeToFile(Path file) throws IOException {
        Files.write(file, Arrays.asList(String.valueOf(offset)));
    }
}
