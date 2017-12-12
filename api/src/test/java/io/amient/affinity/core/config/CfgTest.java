package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class CfgTest {

    public static class ServiceConfig extends ConfigStruct {
        static public ServiceConfig def = new ServiceConfig();
        private Cfg<Class<? extends CfgTest>> Class = cls("class", CfgTest.class, true);
    }

    public static class NodeConfig extends ConfigStruct {
        static public NodeConfig def = new NodeConfig();

        public NodeConfig() {
            super("affinity.node");
        }

        private Cfg<Long> StartupTimeoutMs = longint("startup.timeout.ms", true);
        private Cfg<Long> ShutdownTimeoutMs = longint("shutdown.timeout.ms", true);
        private ConfigGroup<ServiceConfig, Cfg<ServiceConfig>> Services =
                group("service", ServiceConfig.class, false);

    }

    @Rule
    public ExpectedException ex = ExpectedException.none();

    @Test
    public void reportMissingPropertiesOnlyIfRequired() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("shutdown.timeout.ms is required in affinity.node\n" +
                "startup.timeout.ms is required in affinity.node\n");
        Cfg.apply(ConfigFactory.empty(), NodeConfig.def);

    }

    @Test
    public void reportInvalidPropertyForDifferentStruct() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("something.we.dont.recognize is not a known property of affinity.node.service\n" +
                "class is required in affinity.node.service\n");
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.def.StartupTimeoutMs.path, 100L);
            put(NodeConfig.def.ShutdownTimeoutMs.path, 1000L);
            put(NodeConfig.def.Services.path("wrongstruct"), new HashMap<String, Object>() {{
                put("something.we.dont.recognize", 20);
            }});
        }});
        Cfg.apply(config, NodeConfig.def);
    }

    @Test
    public void reportRecoginzieCorrectGroupType() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("com.typesafe.config.Config is not an instance of io.amient.affinity.core.config.CfgTest\n");
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.def.StartupTimeoutMs.path(), 100L);
            put(NodeConfig.def.ShutdownTimeoutMs.path(), 1000L);
            put(NodeConfig.def.Services.path("wrongclass"), new HashMap<String, Object>() {{
                put(ServiceConfig.def.Class.path(), Config.class.getName());
            }});
        }});
        Cfg.apply(config, NodeConfig.def);
    }


    @Test
    public void recognizeCorrectlyConfigured() {
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.def.StartupTimeoutMs.path(), 100L);
            put(NodeConfig.def.ShutdownTimeoutMs.path(), 1000L);
            put(NodeConfig.def.Services.path("myservice"), new HashMap<String, Object>() {{
                put(ServiceConfig.def.Class.path(), CfgTest.class.getName());
            }});
        }});

        assertEquals(CfgTest.class, ((ServiceConfig) (Cfg.apply(config, NodeConfig.def).Services.apply("myservice"))).Class.get());
    }

}
