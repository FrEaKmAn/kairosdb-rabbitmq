package org.kairosdb.plugin.rabbitmq.client;

import com.google.common.base.Strings;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import org.kairosdb.plugin.rabbitmq.datastore.Datastore;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQClientImpl implements RabbitMQClient
{
    /**
     * The RabbitMQ host.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.host")
    private String host = com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;

    /**
     * Default content type for messages when not defined
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.default.content.type")
    private String defaultContentType = "application/json";

    /**
     * The RabbitMQ queue.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.queue")
    private String queue = "kairosdb";

    /**
     * Automatically declare RabbitMQ queue.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.queue.declare")
    private Boolean queueDeclare = true;

    /**
     * The RabbitMQ exchange for rejected messages.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.rejected.exchange")
    private String rejectedExchange = "kairosdb.rejected";

    /**
     * Automatically declare RabbitMQ exchange for rejected messages.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.rejected.exchange.declare")
    private Boolean rejectedExchangeDeclare = true;

    /**
     * The RabbitMQ queue for rejected messages.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.rejected.queue")
    private String rejectedQueue = "kairosdb.rejected";

    /**
     * Automatically declare RabbitMQ queue for rejected messages.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.rejected.queue.declare")
    private Boolean rejectedQueueDeclare = true;

    /**
     * The RabbitMQ virtual host.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.virtual.host")
    private String virtualHost = com.rabbitmq.client.ConnectionFactory.DEFAULT_VHOST;

    /**
     * The RabbitMQ username.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.username")
    private String username = com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;

    /**
     * The RabbitMQ password.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.password")
    private String password = com.rabbitmq.client.ConnectionFactory.DEFAULT_PASS;

    /**
     * The RabbitMQ port.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.port")
    private int port = com.rabbitmq.client.ConnectionFactory.USE_DEFAULT_PORT;

    /**
     * The RabbitMQ connection timeout.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.connection.timeout")
    private int connectionTimeout = com.rabbitmq.client.ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT;

    /**
     * The RabbitMQ channel max.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.requested.channel.max")
    private int requestedChannelMax = com.rabbitmq.client.ConnectionFactory.DEFAULT_CHANNEL_MAX;

    /**
     * The RabbitMQ frame max.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.requested.frame.max")
    private int requestedFrameMax = com.rabbitmq.client.ConnectionFactory.DEFAULT_FRAME_MAX;

    /**
     * The RabbitMQ heartbeat.
     */
    @Inject
    @Named("kairosdb.plugin.rabbitmq.requested.heartbeat")
    private int requestedHeartbeat = com.rabbitmq.client.ConnectionFactory.DEFAULT_HEARTBEAT;

    @Inject
    private Datastore datastore;

    private Connection connection = null;

    @Override
    public void start() throws IOException, TimeoutException
    {
        this.connection = this.createConnection();
        Channel channel = this.createChannel(connection);

        this.createQueue(channel);
        this.createRejectedExchange(channel);
        this.createRejectedQueue(channel);
        this.startConsuming(channel);
    }

    @Override
    public Connection createConnection() throws IOException, TimeoutException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setPort(port);
        factory.setConnectionTimeout(connectionTimeout);
        factory.setRequestedChannelMax(requestedChannelMax);
        factory.setRequestedFrameMax(requestedFrameMax);
        factory.setRequestedHeartbeat(requestedHeartbeat);

        return factory.newConnection();
    }

    @Override
    public Channel createChannel(Connection connection) throws IOException
    {
        return connection.createChannel();
    }

    @Override
    public void createQueue(Channel channel) throws IOException
    {
        if(this.queueDeclare)
        {
            channel.queueDeclare(queue, true, false, false, null);
        }
    }

    @Override
    public void createRejectedExchange(Channel channel) throws IOException
    {
        if(this.rejectedExchangeDeclare && !Strings.isNullOrEmpty(this.rejectedExchange))
        {
            channel.exchangeDeclare(rejectedExchange, "fanout", true);
        }
    }

    @Override
    public void createRejectedQueue(Channel channel) throws IOException
    {
        if(this.rejectedQueueDeclare && !Strings.isNullOrEmpty(this.rejectedExchange) && !Strings.isNullOrEmpty(this.rejectedQueue))
        {
            channel.queueDeclare(rejectedQueue, true, false, false, null);
            channel.queueBind(rejectedQueue, rejectedExchange, "");
        }
    }

    @Override
    public void stop() throws IOException
    {
        if(connection != null && connection.isOpen())
        {
            connection.close();
        }
    }

    @Override
    public void startConsuming(Channel channel) throws IOException
    {
        Consumer consumer = new RabbitMQConsumerImpl(channel, datastore, defaultContentType, rejectedExchange);
        channel.basicConsume(queue, true, consumer);
    }
}
