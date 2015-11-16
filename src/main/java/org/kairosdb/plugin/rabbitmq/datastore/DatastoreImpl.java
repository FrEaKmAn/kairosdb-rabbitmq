package org.kairosdb.plugin.rabbitmq.datastore;

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;

public class DatastoreImpl implements Datastore
{
    @Inject
    private KairosDatastore datastore;

    @Override
    public void save(DataPointSet dps) throws DatastoreException
    {
        for(DataPoint dataPoint: dps.getDataPoints())
        {
            datastore.putDataPoint(dps.getName(), dps.getTags(), dataPoint);
        }
    }
}
