package org.kairosdb.plugin.rabbitmq.client;

import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.plugin.rabbitmq.Message;
import org.kairosdb.plugin.rabbitmq.datastore.Datastore;
import org.kairosdb.plugin.rabbitmq.parsers.CsvParser;
import org.kairosdb.plugin.rabbitmq.parsers.JsonParser;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMQConsumerImpl extends DefaultConsumer implements RabbitMQConsumer
{
    public static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerImpl.class);

    private final Map<String, Parser> parsers = new HashMap<>();
    private final Datastore datastore;
    private final String rejectedExchange;
    private final String defaultContentType;

    public RabbitMQConsumerImpl(Channel channel, Datastore datastore, String defaultContentType, String rejectedExchange)
    {
        super(channel);

        this.datastore = datastore;
        this.defaultContentType = defaultContentType;
        this.rejectedExchange = rejectedExchange;

        this.registerParser(new JsonParser());
        this.registerParser(new CsvParser());
    }

    @Override
    public void registerParser(Parser parser)
    {
        for(String contentType: parser.getContentTypes())
        {
            this.parsers.put(contentType, parser);
        }
    }

    @Override
    public Parser getParser(String contentType) throws Parser.InvalidContentTypeException
    {
        if (contentType == null || contentType.isEmpty())
        {
            contentType = this.defaultContentType;
        }

        if(!this.parsers.containsKey(contentType))
        {
            throw new Parser.InvalidContentTypeException();
        }

        return this.parsers.get(contentType);
    }

    @Override
    public Message parse(byte[] body, String contentType) throws Parser.InvalidContentTypeException, Parser.ParserException
    {
        Parser parser = this.getParser(contentType);
        return parser.parse(body);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        try
        {
            Message message = this.parse(body, properties.getContentType());
            this.save(message);
        }
        catch(Parser.ParserException | NumberFormatException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error parsing message.", ex);
            reject(properties, body);
        }
        catch(Parser.InvalidContentTypeException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error parsing message with content type " + properties.getContentType() + ".", ex);
            reject(properties, body);
        }
        catch (DatastoreException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error storing dataPoints.", ex);
            reject(properties, body);

            // TODO retry?
        }
    }

    @Override
    public void reject(AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        if(!Strings.isNullOrEmpty(this.rejectedExchange))
        {
            getChannel().basicPublish(rejectedExchange, "", properties, body);
        }
    }

    @Override
    public void save(Message message) throws DatastoreException, NumberFormatException
    {
        for(Message.DataPoints dataPoints: message.dataPoints)
        {
            List<DataPoint> values = dataPoints.asDataPoints();
            DataPointSet dps = new DataPointSet(message.metric, dataPoints.tags, values);
            datastore.save(dps);
        }
    }
}
