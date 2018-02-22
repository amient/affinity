package io.amient.affinity.core.util;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.HashMap;
import java.util.Map;

public class ZkClients {

    volatile private static Map<ZkConf, ZkClient> clients = new HashMap<>();
    volatile private static Map<ZkClient, Integer> refs = new HashMap<>();

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
            refs.put(client, 0);
        }
        ZkClient result = clients.get(conf);
        if (!refs.containsKey(result)) throw new IllegalStateException();
        refs.put(result, refs.get(client) + 1);
        return result;
    }

    synchronized public static void close(ZkClient client) {
        if (!refs.containsKey(client)) {
            throw new IllegalStateException();
        } else if (refs.get(client) > 0){
            int refCount = refs.get(client) - 1;
            refs.put(client, refCount);
            if (refCount == 0) {
                client.close();
                refs.remove(client);
            }
        } else {
            throw new IllegalStateException();
        }
    }
}
