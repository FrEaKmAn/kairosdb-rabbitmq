package org.kairosdb.plugin.rabbitmq;

import com.google.gson.annotations.SerializedName;
import org.kairosdb.core.DataPoint;
import org.kairosdb.plugin.rabbitmq.datastore.DataPointFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class Message
{
    public String metric;

    @SerializedName("datapoints")
    public List<DataPoints> dataPoints = new ArrayList<>();

    public static class DataPoints
    {
        public Map<String, String> tags;
        public SortedMap<Long, String> values;

        public List<DataPoint> asDataPoints()
        {
            List<DataPoint> values = new ArrayList<>();
            for(Map.Entry<Long, String> entry: this.values.entrySet())
            {
                Long timestamp = entry.getKey();
                String value = entry.getValue();

                values.add(DataPointFactory.createDataPoint(timestamp, value));
            }

            return values;
        }
    }
}
