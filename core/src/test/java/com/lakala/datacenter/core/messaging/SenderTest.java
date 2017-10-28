package com.lakala.datacenter.core.messaging;

import com.google.gson.Gson;
import com.lakala.datacenter.core.config.ConfigurationLoader;
import com.lakala.datacenter.core.models.ProcessorMessage;
import com.lakala.datacenter.core.models.ProcessorMode;
import com.lakala.datacenter.core.processor.GraphProcessor;
import junit.framework.TestCase;

public class SenderTest extends TestCase {

    private static final String EDGE_LIST_RELATIVE_FILE_PATH = "/neo4j/mazerunner/edgeList.txt";

    public void testSendMessage() throws Exception {
        ConfigurationLoader.testPropertyAccess=true;
        ProcessorMessage processorMessage = new ProcessorMessage("", "strongly_connected_components", ProcessorMode.Partitioned);
        processorMessage.setPath(ConfigurationLoader.getInstance().getHadoopHdfsUri() + GraphProcessor.PROPERTY_GRAPH_UPDATE_PATH);
        // Serialize the processor message
        Gson gson = new Gson();
        String message = gson.toJson(processorMessage);

        // Notify Neo4j that a property update list is available for processing
        Sender.sendMessage(message);
    }


}