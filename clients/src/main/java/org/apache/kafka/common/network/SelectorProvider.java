package org.apache.kafka.common.network;


public class SelectorProvider
{
    private static ThreadLocal<java.nio.channels.spi.SelectorProvider> selectorProviderHolder = new ThreadLocal<>();


    public static void set(java.nio.channels.spi.SelectorProvider selectorProvider)
    {
        selectorProviderHolder.set(selectorProvider);
    }


    public static java.nio.channels.spi.SelectorProvider provider()
    {
        java.nio.channels.spi.SelectorProvider selectorProvider = selectorProviderHolder.get();

        if (selectorProvider != null)
        {
            return selectorProvider;
        }
        else
        {
            return java.nio.channels.spi.SelectorProvider.provider();
        }
    }
}