package org.kairosdb.plugin.rabbitmq.parsers;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.plugin.rabbitmq.Message;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;


public class JsonParserTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private final Parser parser = new JsonParser();

    @Test
    public void shouldParseMessage() throws Parser.ParserException, IOException
    {
        String json = Resources.toString(Resources.getResource("message.json"), Charsets.UTF_8);
        Message message = parser.parse(json.getBytes());

        assertThat(message.metric).isEqualTo("test");
        assertThat(message.dataPoints).hasSize(2);

        assertThat(message.dataPoints.get(0).tags).hasSize(2);
        assertThat(message.dataPoints.get(0).tags.get("name")).isEqualTo("flow");
        assertThat(message.dataPoints.get(0).tags.get("unit")).isEqualTo("m3/min");
        assertThat(message.dataPoints.get(0).values).hasSize(2);
        assertThat(message.dataPoints.get(0).values.get(1388530800000L)).isEqualTo("1.16");
        assertThat(message.dataPoints.get(0).values.get(1388530805000L)).isEqualTo("2.98");

        assertThat(message.dataPoints.get(1).tags).hasSize(2);
        assertThat(message.dataPoints.get(1).tags.get("name")).isEqualTo("pressure");
        assertThat(message.dataPoints.get(1).tags.get("unit")).isEqualTo("barg");
        assertThat(message.dataPoints.get(1).values).hasSize(2);
        assertThat(message.dataPoints.get(1).values.get(1388530800000L)).isEqualTo("7.10");
        assertThat(message.dataPoints.get(1).values.get(1388530805000L)).isEqualTo("7.05");
    }

    @Test(expected = Parser.ParserException.class)
    public void shouldFailToParseNonJsonMessage() throws Parser.ParserException
    {
        String json = "invalid json";
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseMissingMetricMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Metric is missing or empty.");

        String json = Resources.toString(Resources.getResource("message_without_metric.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseEmptyMetricMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Metric is missing or empty.");

        String json = Resources.toString(Resources.getResource("message_empty_metric.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseMissingDataPointsMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Datapoints are missing or empty.");

        String json = Resources.toString(Resources.getResource("message_without_datapoints.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseEmptyDataPointsMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Datapoints are missing or empty.");

        String json = Resources.toString(Resources.getResource("message_empty_datapoints.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseMissingTagsMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Tags are missing or empty.");

        String json = Resources.toString(Resources.getResource("message_without_tags.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseEmptyTagsMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Tags are missing or empty.");

        String json = Resources.toString(Resources.getResource("message_empty_tags.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseMissingValuesMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Values are missing or empty.");

        String json = Resources.toString(Resources.getResource("message_without_values.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseEmptyValuesMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Values are missing or empty.");

        String json = Resources.toString(Resources.getResource("message_empty_values.json"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }
}
