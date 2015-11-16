package org.kairosdb.plugin.rabbitmq.datastore;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;

import static org.assertj.core.api.Assertions.assertThat;

public class DataPointFactoryTest
{
    @Test
    public void shouldCreateDoubleDataPoint()
    {
        DataPoint dataPoint = DataPointFactory.createDataPoint(1L, "20.3");

        assertThat(dataPoint).isInstanceOf(DoubleDataPoint.class);
        assertThat(dataPoint.getTimestamp()).isEqualTo(1L);
        assertThat(dataPoint.getDoubleValue()).isEqualTo(20.3);
    }

    @Test
    public void shouldCreateLongDataPoint()
    {
        DataPoint dataPoint = DataPointFactory.createDataPoint(1L, "15");

        assertThat(dataPoint).isInstanceOf(LongDataPoint.class);
        assertThat(dataPoint.getTimestamp()).isEqualTo(1L);
        assertThat(dataPoint.getLongValue()).isEqualTo(15L);
    }

    @Test
    public void shouldCreateStringDataPoint()
    {
        DataPoint dataPoint = DataPointFactory.createDataPoint(1L, "string");

        assertThat(dataPoint).isInstanceOf(StringDataPoint.class);
        assertThat(dataPoint.getTimestamp()).isEqualTo(1L);
        assertThat(((StringDataPoint)dataPoint).getValue()).isEqualTo("string");
    }
}
