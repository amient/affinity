package io.amient.affinity.core.util;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ZkClients {

    private static final Logger log = LoggerFactory.getLogger(ZkClients.class);

    volatile private static Map<ZkConf, ZkClient> clients = new HashMap<>();
    volatile private static Map<ZkConf, Integer> refs = new HashMap<>();

    synchronized public static ZkClient get(ZkConf conf) {
        ZkClient client = clients.get(conf);
        if (client == null) {
            client = new ZkClient(
                    conf.Connect.apply(),
                    conf.SessionTimeoutMs.apply(),
                    conf.ConnectTimeoutMs.apply(), new ZkSerializer() {
                @Override
                public byte[] serialize(Object o) {
                    return o.toString().getBytes();
                }

                @Override
                public Object deserialize(byte[] bytes) {
                    return new String(bytes);
                }
            });
            clients.put(conf, client);
            refs.put(conf, 0);
        }
        if (!refs.containsKey(conf)) throw new IllegalStateException();
        int refCount = refs.get(conf) + 1;
        log.debug("Opening zkClient refCount=" + refCount + " refs=" + refs.size() + ", conf" + conf);
        refs.put(conf, refCount);
        return client;
    }

    synchronized public static void close(ZkClient client) {
        final AtomicReference<ZkConf> _conf = new AtomicReference();
        clients.forEach((a, b) -> {
            if (client == b) _conf.set(a);
        });
        ZkConf conf = _conf.get();
        if (conf == null || (!refs.containsKey(conf))) {
            throw new IllegalStateException();
        } else if (refs.get(conf) > 0) {
            int refCount = refs.get(conf) - 1;
            refs.put(conf, refCount);
            if (refCount == 0) {
                refs.remove(conf);
                log.debug("Closing zkClient refCount=" + refCount + " refs=" + refs.size() + ", conf" + conf);
                client.close();
                clients.remove(conf);
            }
        } else {
            throw new IllegalStateException();
        }
    }
}
