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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CfgTest {

    public static class NodeConfig extends CfgStruct<NodeConfig> {
        private CfgLong StartupTimeoutMs = longint("startup.timeout.ms", true);
        private CfgLong ShutdownTimeoutMs = longint("shutdown.timeout.ms", true);
        private CfgGroup<ServiceConfig> Services = group("service", ServiceConfig.class, false);

    }

    public static class ServiceConfig extends CfgStruct<ServiceConfig> {
        private CfgCls<TimeCryptoProof> Class = cls("class", TimeCryptoProof.class, true);
        private CfgList IntList = list("intlist", CfgInt.class, false);
        private CfgGroup IntLists = group("lists", CfgIntList.class, false);
        private Cfg Undefined = string("undefined", false);
        private CfgGroup UndefinedGroup = group("undefined-group", UndefinedGroupConfig.class, false);
    }

    public static class UndefinedGroupConfig extends CfgStruct<UndefinedGroupConfig> {
        public UndefinedGroupConfig() {
            super(Options.IGNORE_UNKNOWN);
        }
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
        NodeConfig result = new NodeConfig().apply(config);
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
                put(serviceTemplate.IntLists.path(), new HashMap<String, Object>() {{
                    put("group1", Arrays.asList(1, 2, 3));
                    put("group2", Arrays.asList(4));
                }});
                put(serviceTemplate.UndefinedGroup.path("some.group.member.attribute"), "x");
            }});
        }});

        NodeConfig applied = new NodeConfig().apply(config);
        assertEquals(TimeCryptoProofSHA256.class, applied.Services.apply("myservice").Class.apply());
        assertEquals(Arrays.asList(1,2,3), applied.Services.apply("myservice").IntList.apply());
        assertEquals(Arrays.asList(1,2,3), applied.Services.apply("myservice").IntLists.apply("group1").apply());
        assertEquals(Arrays.asList(4), applied.Services.apply("myservice").IntLists.apply("group2").apply());
        assertTrue(applied.Services.isDefined());
        assertTrue(applied.Services.apply("myservice").Class.isDefined());
        assertTrue(applied.Services.apply("myservice").IntList.isDefined());
        assertFalse(applied.Services.apply("myservice").Undefined.isDefined());
    }

}
