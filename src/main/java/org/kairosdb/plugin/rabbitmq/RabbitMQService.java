package org.kairosdb.plugin.rabbitmq;

import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.plugin.rabbitmq.client.RabbitMQClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQService implements KairosDBService
{
    public static final Logger logger = LoggerFactory.getLogger(RabbitMQService.class);

    @Inject
    public RabbitMQClient client;

    @Override
    public void start() throws KairosDBException
    {
        logger.info("[KairosDB-RabbitMQ] Starting RabbitMQ KairosDB plugin.");

        try
        {
            client.start();

            logger.info("[KairosDB-RabbitMQ] RabbitMQ client successfully started.");
        }
        catch(IOException | TimeoutException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error starting RabbitMQ client.", ex);
        }
    }

    @Override
    public void stop()
    {
        try
        {
            client.stop();

            logger.info("[KairosDB-RabbitMQ] RabbitMQ Connection successfully closed.");
        }
        catch(IOException ex)
        {
            logger.error("[KairosDB-RabbitMQ] Error closing RabbitMQ connection.", ex);
        }
    }
}
