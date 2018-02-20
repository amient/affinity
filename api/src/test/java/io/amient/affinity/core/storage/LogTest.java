package io.amient.affinity.core.storage;

import org.junit.Test;

import java.io.IOException;

public class LogTest {

    @Test
    public void test1() throws IOException {
        StateConf stateConf = new StateConf();
        MemStore kvstore = new MemStoreSimpleMap(stateConf);
        //TODO #110
    }
}
