package org.kairosdb.plugin.rabbitmq;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.kairosdb.plugin.rabbitmq.client.RabbitMQClient;
import org.kairosdb.plugin.rabbitmq.client.RabbitMQClientImpl;
import org.kairosdb.plugin.rabbitmq.datastore.Datastore;
import org.kairosdb.plugin.rabbitmq.datastore.DatastoreImpl;

public class RabbitMQModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(RabbitMQService.class).in(Singleton.class);
        bind(Datastore.class).to(DatastoreImpl.class);
        bind(RabbitMQClient.class).to(RabbitMQClientImpl.class);
    }
}
