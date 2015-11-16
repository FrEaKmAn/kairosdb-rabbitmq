package org.kairosdb.plugin.rabbitmq.parsers;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.kairosdb.plugin.rabbitmq.Message;

/**
 * {
 *   "metric": "test",
 *   "datapoints": [
 *   {
 *     "tags": {"name": "flow", "unit": "m3/min"},
 *     "values": {"1388530800000": "1.16", "1388530805000":"2.98"}
 *   },
 *   {
 *     "tags": {"name": "pressure", "unit": "barg"},
 *     "values": {"1388530800000": "7.10", "1388530805000":"7.05"}
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
            Message message = gson.fromJson(content, Message.class);

            this.validate(message);

            return message;
        }
        catch(Exception ex)
        {
            throw new ParserException(ex);
        }
    }

    public void validate(Message message) throws ParserException
    {
        if(Strings.isNullOrEmpty(message.metric))
        {
            throw new ParserException("Metric is missing or empty.");
        }

        if(message.dataPoints == null || message.dataPoints.isEmpty())
        {
            throw new ParserException("Datapoints are missing or empty.");
        }

        for(Message.DataPoints dataPoints: message.dataPoints)
        {
            if(dataPoints.tags == null || dataPoints.tags.isEmpty())
            {
                throw new ParserException("Tags are missing or empty.");
            }

            if(dataPoints.values == null || dataPoints.values.isEmpty())
            {
                throw new ParserException("Values are missing or empty.");
            }
        }
    }

    @Override
    public String[] getContentTypes()
    {
        return new String[]{ "application/json" };
    }
}
