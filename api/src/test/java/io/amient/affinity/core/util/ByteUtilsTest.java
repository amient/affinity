package io.amient.affinity.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteUtilsTest {
    @Test
    public void parseRadix16ShouldBeReversible() {
        String input = "----020ac416f90d91cffc09b56a9e7aea0420e0cf59----";
        byte[] b = ByteUtils.parseRadix16(input.getBytes(), 4, 40);
        assertEquals("020ac416f90d91cffc09b56a9e7aea0420e0cf59", ByteUtils.toRadix16(b, 0, 20));
    }

    @Test

    public void uuidParserShouldBeCompatibleWithJavaUUID () {
        UUID uuid = UUID.fromString("1c901ed0-b8a1-43ff-ae8e-8e870f603743");
        byte[] parsed1 = ByteUtils.parseUUID(uuid.toString());
        byte[] parsed2 = ByteUtils.uuid(uuid);
        assertTrue(Arrays.equals(parsed2, parsed1));

        UUID converted1 = ByteUtils.uuid(parsed1);
        UUID converted2 = ByteUtils.uuid(parsed2);
        assertEquals(converted1, converted2);
        assertEquals(converted2, uuid);

    }
}
