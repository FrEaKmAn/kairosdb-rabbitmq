package org.kairosdb.plugin.rabbitmq.parsers;

import com.rabbitmq.client.AMQP;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * test;name:flow,unit:m3/min;1388530800000:1.16,1388530805000:2.98;name:pressure,unit:barg;1388530800000:7.10,1388530805000:7.05
 */
public class CsvParser implements Parser
{
    @Override
    public Message parse(byte[] data, AMQP.BasicProperties properties) throws ParserException
    {
        try
        {
            String message = new String(data, "UTF-8");
            String[] parts = message.split(";");
            if(parts.length < 3 || (parts.length-1) % 2 != 0)
            {
                throw new ParserException(message + " is in invalid format.");
            }

            Message datapoints = new Message();
            datapoints.metric = parts[0];
            datapoints.datapoints = this.parseDatapoints(Arrays.copyOfRange(parts, 1, parts.length));

            return datapoints;
        }
        catch(UnsupportedEncodingException ex)
        {
            throw new ParserException(ex);
        }
    }

    protected Map<String, String> parseTags(String part) throws ParserException
    {
        Map<String, String> tags = new HashMap<>();

        String[] parts = part.split(",");
        for(String p: parts)
        {
            String[] keyvalue = p.split(":");
            if(keyvalue.length != 2)
            {
                throw new ParserException(p + " tag is in invalid format.");
            }

            tags.put(keyvalue[0], keyvalue[1]);
        }

        return tags;
    }

    protected SortedMap<Long, String> parseValues(String part) throws ParserException
    {
        SortedMap<Long, String> values = new TreeMap<>();

        String[] parts = part.split(",");
        for(String p: parts)
        {
            String[] keyvalue = p.split(":");
            if(keyvalue.length != 2)
            {
                throw new ParserException(p + " datapoint is in invalid format.");
            }

            try
            {
                values.put(Long.parseLong(keyvalue[0]), keyvalue[1]);
            }
            catch(NumberFormatException ex)
            {
                throw new ParserException(ex);
            }
        }

        return values;
    }

    protected List<Message.DataPoints> parseDatapoints(String[] parts) throws ParserException
    {
        List<Message.DataPoints> datapoints = new ArrayList<>();

        for(int i = 0; i < parts.length; i+=2)
        {
            String tags = parts[i]; // name:flow,unit:m3/min
            String values = parts[i+1]; // 1388530800000:1.16,1388530805000:2.98

            Message.DataPoints messageDataPoints = new Message.DataPoints();
            messageDataPoints.tags = this.parseTags(tags);
            messageDataPoints.values = this.parseValues(values);

            datapoints.add(messageDataPoints);
        }

        return datapoints;
    }
}
