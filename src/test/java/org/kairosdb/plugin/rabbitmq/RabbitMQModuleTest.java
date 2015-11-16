package org.kairosdb.plugin.rabbitmq;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.plugin.rabbitmq.client.RabbitMQClient;
import org.kairosdb.plugin.rabbitmq.client.RabbitMQClientImpl;
import org.kairosdb.plugin.rabbitmq.datastore.Datastore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class RabbitMQModuleTest extends AbstractModule
{
    private final Properties properties = new Properties();

    public RabbitMQModuleTest(Map<String, String> overrideProperties) throws IOException
    {
        InputStream stream = new FileInputStream(Resources.getResource("kairosdb-rabbitmq.properties").getFile());

        this.properties.load(stream);
        for(Map.Entry<String, String> override: overrideProperties.entrySet())
        {
            this.properties.setProperty(override.getKey(), override.getValue());
        }
    }

    @Override
    protected void configure()
    {
        Names.bindProperties(binder(), properties);

        bind(Datastore.class).to(DatastoreTest.class);
        bind(RabbitMQClient.class).to(RabbitMQClientImpl.class);
    }

    public static class DatastoreTest implements Datastore
    {

        @Override
        public void save(DataPointSet dps) throws DatastoreException
        {

        }
    }
}
