package io.amient.affinity.core.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class Checkpoint {

    final public long offset;
    final public boolean closed;

    public Checkpoint(long offset, boolean closed) {
        this.offset = offset;
        this.closed = closed;
    }

    @Override
    public String toString() {
        return "Checkpoint(offset: " + offset + ")";
    }

    public static Checkpoint readFromFile(Path file) throws IOException {
        java.util.List<String> lines = Files.readAllLines(file);
        long offset = Long.valueOf(lines.get(0));
        boolean closed = lines.size() >= 2 && lines.get(1).equals("CLOSED");
        return new Checkpoint(offset, closed);
    }

    public void writeToFile(Path file) throws IOException {
        Files.write(file, Arrays.asList(String.valueOf(offset), closed ? "CLOSED" : "OPEN"));
    }
}
