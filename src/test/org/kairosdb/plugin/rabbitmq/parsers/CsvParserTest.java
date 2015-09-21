package test.org.kairosdb.plugin.rabbitmq.parsers;

import com.rabbitmq.client.AMQP;
import org.junit.Test;
import org.kairosdb.plugin.rabbitmq.parsers.CsvParser;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class CsvParserTest
{
    private final AMQP.BasicProperties properties = new AMQP.BasicProperties();
    private final Parser parser = new CsvParser();

    @Test
    public void testMessageParsing() throws Parser.ParserException, URISyntaxException
    {
        Parser.Message message = parser.parse("test;name:flow,unit:m3/min;1388530800000:1.16,1388530805000:2.98;name:pressure,unit:barg;1388530800000:7.10,1388530805000:7.05".getBytes(), properties);

        assertEquals("test", message.metric);
        assertEquals(2, message.datapoints.size());
        assertEquals("flow", message.datapoints.get(0).tags.get("name"));
        assertEquals("m3/min", message.datapoints.get(0).tags.get("unit"));
        assertEquals("1.16", message.datapoints.get(0).values.get(1388530800000L));
    }
}
