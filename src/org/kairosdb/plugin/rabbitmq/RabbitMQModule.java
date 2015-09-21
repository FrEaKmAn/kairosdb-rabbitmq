package org.kairosdb.plugin.rabbitmq;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class RabbitMQModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(RabbitMQService.class).in(Singleton.class);
    }
}
