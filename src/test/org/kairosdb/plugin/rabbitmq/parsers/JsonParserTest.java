package test.org.kairosdb.plugin.rabbitmq.parsers;

import com.rabbitmq.client.AMQP;
import org.junit.Test;
import org.kairosdb.plugin.rabbitmq.parsers.JsonParser;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;

import static org.junit.Assert.assertEquals;

public class JsonParserTest
{
    private final AMQP.BasicProperties properties = new AMQP.BasicProperties();
    private final Parser parser = new JsonParser();

    @Test
    public void testMessageParsing() throws Parser.ParserException
    {
        Parser.Message message = parser.parse(("{\n" +
                "  metric: \"test\",\n" +
                "  datapoints: [\n" +
                "  {\n" +
                "    tags: {\"name\":\"flow\", \"unit\": \"m3/min\"},\n" +
                "    values: {\"1388530800000\":\"1.16\", \"1388530805000\":\"2.98\"}\n" +
                "  },\n" +
                "  {\n" +
                "    tags: {\"name\":\"pressure\", \"unit\": \"barg\"},\n" +
                "    values: {\"1388530800000\":\"7.10\", \"1388530805000\":\"7.05\"}\n" +
                "  }]\n" +
                "}").getBytes(), properties);

        assertEquals("test", message.metric);

        assertEquals(2, message.datapoints.size());
        assertEquals("flow", message.datapoints.get(0).tags.get("name"));
        assertEquals("m3/min", message.datapoints.get(0).tags.get("unit"));
        assertEquals("1.16", message.datapoints.get(0).values.get(1388530800000L));
    }
}
