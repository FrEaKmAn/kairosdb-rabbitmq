package org.kairosdb.plugin.rabbitmq.client;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface RabbitMQClient
{
    void start() throws IOException, TimeoutException;
    void stop() throws IOException;

    Connection createConnection() throws IOException, TimeoutException;
    Channel createChannel(Connection connection) throws IOException;
    void startConsuming(Channel channel) throws IOException;

    void createQueue(Channel channel) throws IOException;
    void createRejectedExchange(Channel channel) throws IOException;
    void createRejectedQueue(Channel channel) throws IOException;
}
