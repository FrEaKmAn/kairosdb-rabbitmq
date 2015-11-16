package org.kairosdb.plugin.rabbitmq.client;

import com.google.common.collect.ImmutableSortedMap;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.plugin.rabbitmq.Message;
import org.kairosdb.plugin.rabbitmq.datastore.Datastore;
import org.kairosdb.plugin.rabbitmq.parsers.CsvParser;
import org.kairosdb.plugin.rabbitmq.parsers.JsonParser;
import org.kairosdb.plugin.rabbitmq.parsers.Parser;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RabbitMQConsumerTest
{
    @Captor
    ArgumentCaptor<ImmutableSortedMap<String, String>> tagsArgumentCaptor;

    private final Datastore datastore = mock(Datastore.class);
    private final Channel channel = mock(Channel.class);
    private final Parser parser = mock(Parser.class);
    private final Message message = new Message();

    private final String DEFAULT_CONTENT_TYPE = "application/json";
    private final String INVALID_CONTENT_TYPE = "invalid/type";

    private final String DEFAULT_REJECTED_EXCHANGE = "kairosdb.rejected";
    private final String MISSING_REJECTED_QUEUE = null;

    @Test
    public void shouldRegisterParsers() throws Parser.InvalidContentTypeException
    {
        RabbitMQConsumer consumer = new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE);

        assertThat(consumer.getParser("application/json")).isInstanceOf(JsonParser.class);
        assertThat(consumer.getParser("text/csv")).isInstanceOf(CsvParser.class);
    }

    @Test
    public void shouldReturnDefaultParser() throws Parser.InvalidContentTypeException
    {
        RabbitMQConsumer consumer = new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE);
        assertThat(consumer.getParser(DEFAULT_CONTENT_TYPE)).isInstanceOf(JsonParser.class);
    }

    @Test(expected = Parser.InvalidContentTypeException.class)
    public void shouldFailIfInvalidContentType() throws Parser.InvalidContentTypeException
    {
        RabbitMQConsumer consumer = new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE);
        consumer.getParser(INVALID_CONTENT_TYPE);
    }

    @Test
    public void shouldHandleDelivery() throws Parser.ParserException, IOException, Parser.InvalidContentTypeException, DatastoreException
    {
        byte[] body = new byte[0];
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().contentType(DEFAULT_CONTENT_TYPE).build();

        RabbitMQConsumer consumer = spy(new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE));
        doReturn(message).when(consumer).parse(any(byte[].class), anyString());
        doNothing().when(consumer).save(any(Message.class));

        consumer.handleDelivery("", null, properties, body);

        verify(consumer).parse(eq(body), eq(DEFAULT_CONTENT_TYPE));
        verify(consumer).save(eq(message));
    }

    @Test
    public void shouldParseMessage() throws Parser.ParserException, Parser.InvalidContentTypeException
    {
        RabbitMQConsumer consumer = spy(new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE));
        doReturn(parser).when(consumer).getParser(DEFAULT_CONTENT_TYPE);

        byte[] body = new byte[0];
        consumer.parse(body, DEFAULT_CONTENT_TYPE);

        verify(parser).parse(eq(body));
    }

    @Test
    public void shouldSaveMessage() throws DatastoreException
    {
        message.metric = "testmetric";
        message.dataPoints.add(new Message.DataPoints());
        message.dataPoints.get(0).tags = new HashMap<>();
        message.dataPoints.get(0).tags.put("name", "tagname");
        message.dataPoints.get(0).values = new TreeMap<>();
        message.dataPoints.get(0).values.put(1388530800000L, "20.5");
        message.dataPoints.get(0).values.put(1388530805000L, "15");
        message.dataPoints.get(0).values.put(1388530810000L, "string");

        RabbitMQConsumer consumer = spy(new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE));

        consumer.save(message);

        ArgumentCaptor<DataPointSet> argument = ArgumentCaptor.forClass(DataPointSet.class);
        verify(datastore).save(argument.capture());
        assertThat(argument.getValue().getName()).isEqualTo(message.metric);
        assertThat(argument.getValue().getTags()).hasSize(1);
        assertThat(argument.getValue().getTags()).containsEntry("name", "tagname");
        assertThat(argument.getValue().getDataPoints()).hasSize(3);
        assertThat(argument.getValue().getDataPoints()).contains(new DoubleDataPoint(1388530800000L, 20.5));
        assertThat(argument.getValue().getDataPoints()).contains(new LongDataPoint(1388530805000L, 15L));
        assertThat(argument.getValue().getDataPoints()).contains(new StringDataPoint(1388530810000L, "string"));
    }

    @Test
    public void shouldRejectMessage() throws IOException
    {
        byte[] body = new byte[0];
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().contentType(DEFAULT_CONTENT_TYPE).build();

        RabbitMQConsumer consumer = spy(new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, DEFAULT_REJECTED_EXCHANGE));
        doReturn(channel).when(consumer).getChannel();

        consumer.reject(properties, body);

        verify(channel).basicPublish(eq(DEFAULT_REJECTED_EXCHANGE), eq(""),eq(properties), eq(body));
    }

    @Test
    public void shouldSkipToRejectMessageIfRejectedExchangeIsMissing() throws IOException
    {
        byte[] body = new byte[0];
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().contentType(DEFAULT_CONTENT_TYPE).build();

        RabbitMQConsumer consumer = spy(new RabbitMQConsumerImpl(channel, datastore, DEFAULT_CONTENT_TYPE, MISSING_REJECTED_QUEUE));
        doReturn(channel).when(consumer).getChannel();

        consumer.reject(properties, body);

        verify(channel, never()).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), any(byte[].class));
    }
}
