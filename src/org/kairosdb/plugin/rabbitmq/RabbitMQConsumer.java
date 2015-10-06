package org.kairosdb.plugin.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.lang3.math.NumberUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.plugin.rabbitmq.parsers.CsvParser;
import org.kairosdb.plugin.rabbitmq.parsers.JsonParser;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMQConsumer extends DefaultConsumer
{
    public static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

    private final Map<String, Parser> parsers = new HashMap<>();
    private final KairosDatastore datastore;
    private final String rejectedExchange;

    public RabbitMQConsumer(KairosDatastore datastore, Channel channel, String rejectedExchange)
    {
        super(channel);

        this.datastore = datastore;
        this.rejectedExchange = rejectedExchange;

        this.registerParser(new JsonParser());
        this.registerParser(new CsvParser());
    }

    public void registerParser(Parser parser)
    {
        for(String contentType: parser.getContentTypes())
        {
            this.parsers.put(contentType, parser);
        }
    }

    public Parser getParser(String contentType) throws Parser.InvalidContentTypeException
    {
        if (contentType == null || contentType.isEmpty())
        {
            contentType = "application/json";
        }

        if(!this.parsers.containsKey(contentType))
        {
            throw new Parser.InvalidContentTypeException();
        }

        return this.parsers.get(contentType);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        try
        {
            Parser parser = getParser(properties.getContentType());
            Parser.Message dataPoints = parser.parse(body);
            save(dataPoints);
        }
        catch(Parser.ParserException | NumberFormatException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error parsing message.", ex);
            getChannel().basicPublish(rejectedExchange, "", properties, body);
        }
        catch(Parser.InvalidContentTypeException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error parsing message with content type " + properties.getContentType() + ".", ex);
            getChannel().basicPublish(rejectedExchange, "", properties, body);
        }
        catch (DatastoreException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error storing datapoints.", ex);
            getChannel().basicPublish(rejectedExchange, "", properties, body);

            // TODO retry?
        }
    }

    public void save(Parser.Message message) throws DatastoreException, NumberFormatException
    {
        for(Parser.Message.DataPoints dataPoints: message.datapoints)
        {
            List<DataPoint> values = new ArrayList<>();

            for(Map.Entry<Long, String> entry: dataPoints.values.entrySet())
            {
                String value = entry.getValue();
                Long timestamp = entry.getKey();

                if(NumberUtils.isNumber(value))
                {
                    if (value.contains("."))
                    {
                        values.add(new DoubleDataPoint(timestamp, Double.parseDouble(value)));
                    }
                    else
                    {
                        values.add(new LongDataPoint(timestamp, Long.parseLong(value)));
                    }
                }
                else
                {
                    values.add(new StringDataPoint(timestamp, value));
                }
            }

            DataPointSet dps = new DataPointSet(message.metric, dataPoints.tags, values);
            for (DataPoint dataPoint : dps.getDataPoints())
            {
                datastore.putDataPoint(dps.getName(), dps.getTags(), dataPoint);
            }
        }
    }
}
