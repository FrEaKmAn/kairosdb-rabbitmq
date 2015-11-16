package org.kairosdb.plugin.rabbitmq.parsers;

import org.kairosdb.plugin.rabbitmq.Message;

public interface Parser
{
    Message parse(byte[] data) throws ParserException;
    String[] getContentTypes();

    class ParserException extends Exception
    {
        public ParserException(Throwable throwable)
        {
            super(throwable);
        }

        public ParserException(String message)
        {
            super(message);
        }
    }

    class InvalidContentTypeException extends Exception
    {
    }
}
