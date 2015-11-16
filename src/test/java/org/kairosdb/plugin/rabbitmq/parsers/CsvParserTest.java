package org.kairosdb.plugin.rabbitmq.parsers;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.plugin.rabbitmq.Message;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;


public class CsvParserTest
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private final Parser parser = new CsvParser();

    @Test
    public void shouldParseMessage() throws Parser.ParserException, IOException
    {
        String csv = Resources.toString(Resources.getResource("message.csv"), Charsets.UTF_8);
        Message message = parser.parse(csv.getBytes());

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

    @Test
    public void shouldFailToParseNonCsvMessage() throws Parser.ParserException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("invalid csv is in invalid format.");

        String csv = "invalid csv";
        parser.parse(csv.getBytes());
    }

    @Test
    public void shouldFailToParseMissingMetricMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Metric is empty.");

        String json = Resources.toString(Resources.getResource("message_without_metric.csv"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseMissingTagsMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("test;1388530800000:1.16,1388530805000:2.98 is in invalid format.");

        String json = Resources.toString(Resources.getResource("message_without_tags.csv"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseEmptyTagsMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Tags are empty.");

        String json = Resources.toString(Resources.getResource("message_empty_tags.csv"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseInvalidTagsFormatMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("name tag has invalid format.");

        String json = Resources.toString(Resources.getResource("message_invalid_tags_format.csv"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseEmptyValuesMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("Values are empty.");

        String json = Resources.toString(Resources.getResource("message_empty_values.csv"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }

    @Test
    public void shouldFailToParseSwitchedTagsAndValuesMessage() throws Parser.ParserException, IOException
    {
        expectedEx.expect(Parser.ParserException.class);
        expectedEx.expectMessage("java.lang.NumberFormatException: For input string: \"name\"");

        String json = Resources.toString(Resources.getResource("message_switched_tags_and_values.csv"), Charsets.UTF_8);
        parser.parse(json.getBytes());
    }
}
