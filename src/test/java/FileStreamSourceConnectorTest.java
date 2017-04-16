import org.apache.kafka.common.config.ConfigDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Sudipto Saha on 4/15/2017.
 */
public class FileStreamSourceConnectorTest {
    private FileStreamSourceConnector fileStreamSourceConnector;
    private String version;
    private Map<String,String> sourceProperties;
    private static final String TOPIC="topic";
    private static final String TOPIC_MULTIPLE="topic1,topic2";
    private static final String FILENAME="filename";

    @Before
    public void setUp() throws Exception {
        fileStreamSourceConnector=new FileStreamSourceConnector();
        version="0.10.2.0";
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void version() throws Exception {
        Assert.assertEquals(fileStreamSourceConnector.version(),version);
    }

    @Test
    public void start() throws Exception {
        sourceProperties = new HashMap<String, String>();
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG,TOPIC);
        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG,FILENAME);
        fileStreamSourceConnector.start(sourceProperties);

    }

    @Test(expected = org.apache.kafka.connect.errors.ConnectException.class)
    public void start1() throws Exception {
        sourceProperties = new HashMap<String, String>();
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG,TOPIC_MULTIPLE);
        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG,FILENAME);
        fileStreamSourceConnector.start(sourceProperties);

    }

    @Test()
    public void taskClass() throws Exception {

        fileStreamSourceConnector.taskClass();
    }

    @Test
    public void taskConfigs() throws Exception {

        List<Map<String,String>> taskConfigs=fileStreamSourceConnector.taskConfigs(1);
        Assert.assertEquals(1,taskConfigs.size());
        /*Assert.assertEquals(FILENAME,taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
        Assert.assertEquals(TOPIC,taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));*/
    }

    @Test
    public void stop() throws Exception {
        fileStreamSourceConnector.stop();
    }

    @Test()
    public void config() throws Exception {
        ConfigDef configDef=fileStreamSourceConnector.config();
        Assert.assertEquals(configDef,null);
    }

}