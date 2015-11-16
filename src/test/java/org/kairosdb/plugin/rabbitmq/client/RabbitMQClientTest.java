package org.kairosdb.plugin.rabbitmq.client;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Test;
import org.kairosdb.plugin.rabbitmq.RabbitMQModuleTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class RabbitMQClientTest
{
    private final Map<String, String> properties = new HashMap<>();

    private final Connection connection = mock(Connection.class);
    private final Channel channel = mock(Channel.class);

    private final String DEFAULT_QUEUE = "kairosdb";
    private final String DEFAULT_REJECTED_EXCHANGE = "kairosdb.rejected";
    private final String DEFAULT_REJECTED_EXCHANGE_TYPE = "fanout";
    private final String DEFAULT_REJECTED_QUEUE = "kairosdb.rejected";

    protected RabbitMQClient client() throws IOException
    {
        Injector injector = Guice.createInjector(new RabbitMQModuleTest(properties));
        return injector.getInstance(RabbitMQClient.class);
    }

    @Test
    public void shouldCreateChannel() throws IOException
    {
        this.client().createChannel(connection);

        verify(connection).createChannel();
    }

    @Test
    public void shouldStartClient() throws IOException, TimeoutException
    {
        RabbitMQClient client = spy(this.client());

        doReturn(connection).when(client).createConnection();
        doReturn(channel).when(client).createChannel(any(Connection.class));

        doNothing().when(client).createQueue(any(Channel.class));
        doNothing().when(client).createRejectedExchange(any(Channel.class));
        doNothing().when(client).createRejectedQueue(any(Channel.class));
        doNothing().when(client).startConsuming(any(Channel.class));

        client.start();

        verify(client).createConnection();
        verify(client).createChannel(eq(connection));
        verify(client).createQueue(eq(channel));
        verify(client).createRejectedExchange(eq(channel));
        verify(client).createRejectedQueue(eq(channel));
        verify(client).startConsuming(eq(channel));
    }

    @Test
    public void shouldStopClient() throws IOException, TimeoutException
    {
        RabbitMQClient client = spy(this.client());

        when(connection.isOpen()).thenReturn(true);

        doReturn(connection).when(client).createConnection();
        doReturn(channel).when(client).createChannel(any(Connection.class));
        doNothing().when(client).createQueue(any(Channel.class));
        doNothing().when(client).createRejectedExchange(any(Channel.class));
        doNothing().when(client).createRejectedQueue(any(Channel.class));
        doNothing().when(client).startConsuming(any(Channel.class));

        client.start();
        client.stop();

        verify(connection).close();
    }

    @Test
    public void shouldSkipToStopIfClientNeverStarted() throws IOException, TimeoutException
    {
        RabbitMQClient client = spy(this.client());

        client.stop();

        verify(connection, never()).close();
    }

    @Test
    public void shouldDeclareQueue() throws IOException
    {
        properties.put("kairosdb.plugin.rabbitmq.queue", DEFAULT_QUEUE);
        properties.put("kairosdb.plugin.rabbitmq.queue.declare", "true");

        this.client().createQueue(channel);

        verify(channel).queueDeclare(eq(DEFAULT_QUEUE), eq(true), eq(false), eq(false), isNull(Map.class));
    }

    @Test
    public void shouldNotDeclareQueueIfDisabledInConfiguration() throws IOException
    {
        properties.put("kairosdb.plugin.rabbitmq.queue", DEFAULT_QUEUE);
        properties.put("kairosdb.plugin.rabbitmq.queue.declare", "false");

        this.client().createQueue(channel);

        verify(channel, never()).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMapOf(String.class, Object.class));
    }

    @Test
    public void shouldDeclareRejectedExchange() throws IOException
    {
        properties.put("kairosdb.plugin.rabbitmq.rejected.exchange", DEFAULT_REJECTED_EXCHANGE);
        properties.put("kairosdb.plugin.rabbitmq.rejected.exchange.declare", "true");

        this.client().createRejectedExchange(channel);

        verify(channel).exchangeDeclare(eq(DEFAULT_REJECTED_EXCHANGE), eq(DEFAULT_REJECTED_EXCHANGE_TYPE), eq(true));
    }

    @Test
    public void shouldNotDeclareRejectedExchangeIfDisabledInConfiguration() throws IOException
    {
        properties.put("kairosdb.plugin.rabbitmq.rejected.exchange", DEFAULT_REJECTED_EXCHANGE);
        properties.put("kairosdb.plugin.rabbitmq.rejected.exchange.declare", "false");

        this.client().createRejectedExchange(channel);

        verify(channel, never()).exchangeDeclare(anyString(), anyString(), anyBoolean());
    }

    @Test
    public void shouldDeclareRejectedQueue() throws IOException
    {
        properties.put("kairosdb.plugin.rabbitmq.rejected.exchange", DEFAULT_REJECTED_EXCHANGE);
        properties.put("kairosdb.plugin.rabbitmq.rejected.queue", DEFAULT_REJECTED_QUEUE);
        properties.put("kairosdb.plugin.rabbitmq.rejected.queue.declare", "true");

        this.client().createRejectedQueue(channel);

        verify(channel).queueDeclare(eq(DEFAULT_REJECTED_QUEUE), eq(true), eq(false), eq(false), isNull(Map.class));
        verify(channel).queueBind(eq(DEFAULT_REJECTED_QUEUE), eq(DEFAULT_REJECTED_EXCHANGE), eq(""));
    }

    @Test
    public void shouldNotDeclareRejectedQueueIfDisabledInConfiguration() throws IOException
    {
        properties.put("kairosdb.plugin.rabbitmq.rejected.exchange", DEFAULT_REJECTED_EXCHANGE);
        properties.put("kairosdb.plugin.rabbitmq.rejected.queue", DEFAULT_REJECTED_QUEUE);
        properties.put("kairosdb.plugin.rabbitmq.rejected.queue.declare", "false");

        this.client().createRejectedExchange(channel);

        verify(channel, never()).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMapOf(String.class, Object.class));
        verify(channel, never()).queueBind(anyString(), anyString(), anyString());
    }
}
