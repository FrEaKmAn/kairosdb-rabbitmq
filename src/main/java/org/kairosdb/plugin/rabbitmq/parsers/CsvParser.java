package org.kairosdb.plugin.rabbitmq.parsers;

import com.google.common.base.Strings;
import org.kairosdb.plugin.rabbitmq.Message;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * test;name:flow,unit:m3/min;1388530800000:1.16,1388530805000:2.98;name:pressure,unit:barg;1388530800000:7.10,1388530805000:7.05
 */
public class CsvParser implements Parser
{
    @Override
    public Message parse(byte[] data) throws ParserException
    {
        try
        {
            String message = new String(data, "UTF-8");
            String[] parts = message.split(";");
            if(parts.length < 3 || (parts.length-1) % 2 != 0)
            {
                throw new ParserException(message + " is in invalid format.");
            }

            if(Strings.isNullOrEmpty(parts[0]))
            {
                throw new ParserException("Metric is empty.");
            }

            Message datapoints = new Message();
            datapoints.metric = parts[0];
            datapoints.dataPoints = this.parseDatapoints(Arrays.copyOfRange(parts, 1, parts.length));

            return datapoints;
        }
        catch(UnsupportedEncodingException ex)
        {
            throw new ParserException(ex);
        }
    }

    @Override
    public String[] getContentTypes()
    {
        return new String[]{ "text/csv" };
    }

    protected Map<String, String> parseTags(String part) throws ParserException
    {
        if(Strings.isNullOrEmpty(part))
        {
            throw new ParserException("Tags are empty.");
        }

        Map<String, String> tags = new HashMap<>();

        String[] parts = part.split(",");
        for(String p: parts)
        {
            String[] keyvalue = p.split(":");
            if(keyvalue.length != 2)
            {
                throw new ParserException(p + " tag has invalid format.");
            }

            tags.put(keyvalue[0], keyvalue[1]);
        }

        return tags;
    }

    protected SortedMap<Long, String> parseValues(String part) throws ParserException
    {
        if(Strings.isNullOrEmpty(part))
        {
            throw new ParserException("Values are empty.");
        }

        SortedMap<Long, String> values = new TreeMap<>();

        String[] parts = part.split(",");
        for(String p: parts)
        {
            String[] keyvalue = p.split(":");
            if(keyvalue.length != 2)
            {
                throw new ParserException(p + " datapoint has invalid format.");
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
        List<Message.DataPoints> dataPoints = new ArrayList<>();

        for(int i = 0; i < parts.length; i+=2)
        {
            String tags = parts[i]; // name:flow,unit:m3/min
            String values = parts[i+1]; // 1388530800000:1.16,1388530805000:2.98

            Message.DataPoints messageDataPoints = new Message.DataPoints();
            messageDataPoints.tags = this.parseTags(tags);
            messageDataPoints.values = this.parseValues(values);

            dataPoints.add(messageDataPoints);
        }

        return dataPoints;
    }
}
