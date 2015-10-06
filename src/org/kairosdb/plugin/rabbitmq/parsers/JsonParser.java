package org.kairosdb.plugin.rabbitmq.parsers;

import com.google.gson.Gson;

/**
 * {
 *   metric: "test",
 *   datapoints: [
 *   {
 *     tags: {"name":"flow", "unit": "m3/min"},
 *     values: {"1388530800000":"1.16", "1388530805000":"2.98"}
 *   },
 *   {
 *     tags: {"name":"pressure", "unit": "barg"},
 *     values: {"1388530800000":"7.10", "1388530805000":"7.05"}
 *   }]
 * }
 */
public class JsonParser implements Parser
{
    private final Gson gson;

    public JsonParser()
    {
        this.gson = new Gson();
    }

    @Override
    public Message parse(byte[] data) throws ParserException
    {
        try
        {
            String content = new String(data, "UTF-8");
            return gson.fromJson(content, Message.class);
        }
        catch(Exception ex)
        {
            throw new ParserException(ex);
        }
    }

    @Override
    public String[] getContentTypes()
    {
        return new String[]{ "application/json" };
    }
}
