package org.kairosdb.plugin.rabbitmq.client;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.plugin.rabbitmq.Message;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;

import java.io.IOException;

public interface RabbitMQConsumer
{
    void registerParser(Parser parser);
    Parser getParser(String contentType) throws Parser.InvalidContentTypeException;

    void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException;
    void reject(AMQP.BasicProperties properties, byte[] body) throws IOException;
    Channel getChannel();

    Message parse(byte[] body, String contentType) throws Parser.InvalidContentTypeException, Parser.ParserException;

    void save(Message message) throws DatastoreException, NumberFormatException;
}
