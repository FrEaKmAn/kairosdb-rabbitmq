package org.kairosdb.plugin.rabbitmq;

import com.rabbitmq.client.*;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQService implements KairosDBService
{
    public static final Logger logger = LoggerFactory.getLogger(RabbitMQService.class);

    /**
     * The RabbitMQ host.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.host")
    private String rabbitMQHost = com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;

    /**
     * The RabbitMQ queue.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.queue")
    private String rabbitMQQueue = "kairosdb";

    /**
     * The RabbitMQ virtual host.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.virtualhost")
    private String rabbitMQVirtualHost = com.rabbitmq.client.ConnectionFactory.DEFAULT_VHOST;

    /**
     * The RabbitMQ user.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.username")
    private String rabbitMQUser = com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;

    /**
     * The RabbitMQ password.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.password")
    private String rabbitMQPassword = com.rabbitmq.client.ConnectionFactory.DEFAULT_PASS;

    /**
     * The RabbitMQ port.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.port")
    private int rabbitMQPort = com.rabbitmq.client.ConnectionFactory.USE_DEFAULT_PORT;

    /**
     * The RabbitMQ timeout.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.connectionTimeout")
    private int rabbitMQTimeout = com.rabbitmq.client.ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT;

    /**
     * The RabbitMQ channel max.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.requestedChannelMax")
    private int rabbitMQChannelMax = com.rabbitmq.client.ConnectionFactory.DEFAULT_CHANNEL_MAX;

    /**
     * The RabbitMQ frame max.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.requestedFrameMax")
    private int rabbitMQFrameMax = com.rabbitmq.client.ConnectionFactory.DEFAULT_FRAME_MAX;

    /**
     * The RabbitMQ heartbeat.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.requestedHeartbeat")
    private int rabbitMQHeartbeat = com.rabbitmq.client.ConnectionFactory.DEFAULT_HEARTBEAT;

    @Inject
    private KairosDatastore datastore;

    private Connection connection = null;

    @Override
    public void start() throws KairosDBException
    {
        logger.info("[KairosDB-RabbitMQ] Starting RabbitMQ KairosDB plugin.");

        try
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(rabbitMQHost);
            factory.setVirtualHost(rabbitMQVirtualHost);
            factory.setUsername(rabbitMQUser);
            factory.setPassword(rabbitMQPassword);
            factory.setPort(rabbitMQPort);
            factory.setConnectionTimeout(rabbitMQTimeout);
            factory.setRequestedChannelMax(rabbitMQChannelMax);
            factory.setRequestedFrameMax(rabbitMQFrameMax);
            factory.setRequestedHeartbeat(rabbitMQHeartbeat);

            connection = factory.newConnection();
            Channel channel = connection.createChannel();

            Consumer consumer = new RabbitMQConsumer(datastore, channel);
            channel.basicConsume(rabbitMQQueue, false, consumer);
        }
        catch(IOException | TimeoutException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error.", ex);
        }
    }

    @Override
    public void stop()
    {
        if(connection != null && connection.isOpen())
        {
            try
            {
                connection.close();
                logger.error("[KairosDB-RabbitMQ] Connection successfully closed.");
            }
            catch(IOException ex)
            {
                logger.error("[KairosDB-RabbitMQ] Error closing connection.", ex);
            }
        }
    }
}
