package org.kairosdb.plugin.rabbitmq.datastore;

import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.exception.DatastoreException;

public interface Datastore
{
    void save(DataPointSet dps) throws DatastoreException;
}
