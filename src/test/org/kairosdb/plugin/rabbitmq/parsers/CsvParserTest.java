package test.org.kairosdb.plugin.rabbitmq.parsers;

import org.junit.Test;
import org.kairosdb.plugin.rabbitmq.parsers.CsvParser;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class CsvParserTest
{
    private final Parser parser = new CsvParser();

    @Test
    public void testMessageParsing() throws Parser.ParserException, URISyntaxException
    {
        String csv = "test;name:flow,unit:m3/min;1388530800000:1.16,1388530805000:2.98;name:pressure,unit:barg;1388530800000:7.10,1388530805000:7.05";
        Parser.Message message = parser.parse(csv.getBytes());

        assertEquals("test", message.metric);
        assertEquals(2, message.datapoints.size());
        assertEquals("flow", message.datapoints.get(0).tags.get("name"));
        assertEquals("m3/min", message.datapoints.get(0).tags.get("unit"));
        assertEquals("1.16", message.datapoints.get(0).values.get(1388530800000L));
    }
}
