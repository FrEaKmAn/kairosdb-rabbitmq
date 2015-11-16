package org.kairosdb.plugin.rabbitmq.datastore;

import org.apache.commons.lang3.math.NumberUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;

public class DataPointFactory
{
    public static DataPoint createDataPoint(Long timestamp, String value)
    {
        if(NumberUtils.isNumber(value))
        {
            if (value.contains("."))
            {
                return new DoubleDataPoint(timestamp, Double.parseDouble(value));
            }

            return new LongDataPoint(timestamp, Long.parseLong(value));
        }

        return new StringDataPoint(timestamp, value);
    }
}
