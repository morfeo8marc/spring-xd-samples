package org.springframework.xd.spark.streaming.java;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.xd.dirt.server.singlenode.SingleNodeApplication;
import org.springframework.xd.dirt.test.SingleNodeIntegrationTestSupport;
import org.springframework.xd.dirt.test.SingletonModuleRegistry;
import org.springframework.xd.dirt.test.process.SingleNodeProcessingChain;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.test.RandomConfigurationSupport;
import org.springframework.xd.tuple.DefaultTuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.xd.dirt.test.process.SingleNodeProcessingChainSupport.chain;

/**
 *
 *
 * @author toffen.
 */
public class WordCountTest {

    private static SingleNodeApplication application;

    private static int RECEIVE_TIMEOUT = 5000;

    private static String MODULE_NAME = "java-word-count";


    /**
     * Start the single node container, binding random unused ports, etc. to not conflict with any other instances
     * running on this host. Configure the ModuleRegistry to include the project module.
     */
    @BeforeClass
    public static void setUp() {
        RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
        application = new SingleNodeApplication().run();
        SingleNodeIntegrationTestSupport singleNodeIntegrationTestSupport = new SingleNodeIntegrationTestSupport (application);
        singleNodeIntegrationTestSupport.addModuleRegistry(new SingletonModuleRegistry(ModuleType.processor, MODULE_NAME));

    }

    @Test
    public void test() {
        String streamName = "wordCountTest";
        String json = "foo foo foo bar" ;

        String processingChainUnderTest = MODULE_NAME;

        SingleNodeProcessingChain chain = chain(application, streamName, processingChainUnderTest);

        chain.sendPayload(json);

        DefaultTuple result = (DefaultTuple) chain.receivePayload(RECEIVE_TIMEOUT);


        assertEquals(result.getString("foo"), 3);
        //Unbind the source and sink channels from the message bus
        chain.destroy();
    }
}
