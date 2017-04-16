import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Sudipto Saha on 4/15/2017.
 */
public class FileStreamSourceTaskTest {

    private FileStreamSourceConnector fileStreamSourceConnector;
    private  FileStreamSourceTask fileStreamSourceTask;
    private static final String TOPIC = "topic";
    public static final String FILENAME_FIELD = "filename";
    private String filename;
    private File tempFile;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;

    @Before
    public void setUp() throws Exception {
        fileStreamSourceConnector = new FileStreamSourceConnector();
        fileStreamSourceTask = new FileStreamSourceTask();
        tempFile = File.createTempFile("./test.txt",null);

        context = Mockito.mock(SourceTaskContext.class);
        offsetStorageReader = Mockito.mock(OffsetStorageReader.class);
        fileStreamSourceTask.initialize(context);
        Map<String,String> map = new HashMap<String, String>();
        map.put(FILENAME_FIELD,tempFile.getAbsolutePath());
        Mockito.doReturn(offsetStorageReader).when(context).offsetStorageReader();
        Mockito.doReturn(map).when(offsetStorageReader).offset(Collections.singletonMap(FILENAME_FIELD, filename));

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void version() throws Exception {
        String expected = "0.10.2.0";
        String actual = fileStreamSourceTask.version();
        Assert.assertEquals(expected,actual);
    }

    @Test
    public void start() throws Exception {

        Map<String,String> props = new HashMap<String, String>();
        props.put(FileStreamSourceConnector.TOPIC_CONFIG,TOPIC);
        props.put(FileStreamSourceConnector.FILE_CONFIG,tempFile.getAbsolutePath());
        fileStreamSourceTask.start(props);

    }

    @Test(expected = ConnectException.class)
    public void start1() throws Exception {

        Map<String,String> props = new HashMap<String, String>();
        props.put(FileStreamSourceConnector.TOPIC_CONFIG,null);
        props.put(FileStreamSourceConnector.FILE_CONFIG,tempFile.getAbsolutePath());
        fileStreamSourceTask.start(props);

    }

    @Test
    public void start2() throws Exception {

        Map<String,String> props = new HashMap<String, String>();
        props.put(FileStreamSourceConnector.TOPIC_CONFIG,TOPIC);
        props.put(FileStreamSourceConnector.FILE_CONFIG,null);
        fileStreamSourceTask.start(props);

    }

    @Test
    public void poll() throws Exception {

        Map<String,String> props = new HashMap<String, String>();
        props.put(FileStreamSourceConnector.TOPIC_CONFIG,TOPIC);
        props.put(FileStreamSourceConnector.FILE_CONFIG,tempFile.getAbsolutePath());
        fileStreamSourceTask.start(props);

        Assert.assertEquals(null,fileStreamSourceTask.poll());

        FileOutputStream os = new FileOutputStream(tempFile);
        os.write("Sapient Global Market\n".getBytes());
        os.flush();
        List<SourceRecord> records = fileStreamSourceTask.poll();
        Assert.assertEquals(1,records.size());
        Assert.assertEquals("Sapient Global Market",records.get(0).value());
        Assert.assertEquals(TOPIC,records.get(0).topic());
        Assert.assertEquals(Collections.singletonMap("position",22L),records.get(0).sourceOffset());

        os.write("a\nb\nc\nd\n".getBytes());
        os.flush();
        List<SourceRecord> record1 = fileStreamSourceTask.poll();
        Assert.assertEquals(4,record1.size());
        Assert.assertEquals("b",record1.get(1).value());




    }

    @Test
    public void stop() throws Exception {
        Map<String,String> props = new HashMap<String, String>();
        props.put(FileStreamSourceConnector.TOPIC_CONFIG,TOPIC);
        props.put(FileStreamSourceConnector.FILE_CONFIG,tempFile.getAbsolutePath());
        fileStreamSourceTask.start(props);
        fileStreamSourceTask.stop();
    }

}