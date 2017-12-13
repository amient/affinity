package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.amient.affinity.core.util.TimeCryptoProof;
import io.amient.affinity.core.util.TimeCryptoProofSHA256;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class CfgTest {

    public static class ServiceConfig extends CfgStruct<ServiceConfig> {
        private Cfg<Class<? extends TimeCryptoProof>> Class = cls("class", TimeCryptoProof.class, true);
        private CfgList IntList = list("intlist", CfgInt.class, false);

    }

    public static class NodeConfig extends CfgStruct<NodeConfig> {
        private Cfg<Long> StartupTimeoutMs = longint("startup.timeout.ms", true);
        private Cfg<Long> ShutdownTimeoutMs = longint("shutdown.timeout.ms", true);
        private CfgGroup<ServiceConfig> Services = group("service", ServiceConfig.class, false);

    }

    @Rule
    public ExpectedException ex = ExpectedException.none();

    @Test
    public void reportMissingPropertiesOnlyIfRequired() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("startup.timeout.ms is required\n" +
                "shutdown.timeout.ms is required\n");
        new NodeConfig().apply(ConfigFactory.empty());

    }

    @Test
    public void reportInvalidPropertyForDifferentStruct() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("class is required in service.wrongstruct\n" +
                "something.we.dont.recognize is not a known property of service.wrongstruct\n");
        NodeConfig template = new NodeConfig();
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(template.StartupTimeoutMs.path(), 100L);
            put(template.ShutdownTimeoutMs.path(), 1000L);
            put(template.Services.path("wrongstruct"), new HashMap<String, Object>() {{
                put("something.we.dont.recognize", 20);
            }});
        }});
        new NodeConfig().apply(config);
    }

    @Test
    public void reportRecoginzieCorrectGroupType() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("com.typesafe.config.Config is not an instance of class io.amient.affinity.core.util.TimeCryptoProof\n");
        NodeConfig template = new NodeConfig();
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(template.StartupTimeoutMs.path(), 100L);
            put(template.ShutdownTimeoutMs.path(), 1000L);
            put(template.Services.path("wrongclass"), new HashMap<String, Object>() {{
                put(new ServiceConfig().Class.path(), Config.class.getName());
            }});
        }});
        new NodeConfig().apply(config);
    }


    @Test
    public void recognizeCorrectlyConfigured() {
        NodeConfig nodeTemplate = new NodeConfig();
        ServiceConfig serviceTemplate = new ServiceConfig();
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(nodeTemplate.StartupTimeoutMs.path(), 100L);
            put(nodeTemplate.ShutdownTimeoutMs.path(), 1000L);
            put(nodeTemplate.Services.path("myservice"), new HashMap<String, Object>() {{
                put(serviceTemplate.Class.path(), TimeCryptoProofSHA256.class.getName());
                put(serviceTemplate.IntList.path(), Arrays.asList(1,2,3));
            }});
        }});

        NodeConfig v = new NodeConfig().apply(config);
        assertEquals(TimeCryptoProofSHA256.class, v.Services.apply("myservice").Class.apply());
        assertEquals(Arrays.asList(1,2,3), v.Services.apply("myservice").IntList.apply());
    }

}
