package org.apache.dolphinscheduler.network;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractRpcService {

    protected EventLoopGroup newEpollEventLoopGroup(int nThreads, String threadNamePrefix) {
        return new EpollEventLoopGroup(nThreads, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format(threadNamePrefix + "%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    protected EventLoopGroup newNioEventLoopGroup(int nThreads, String threadNamePrefix) {
        return new NioEventLoopGroup(nThreads, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format(threadNamePrefix + "%d", this.threadIndex.incrementAndGet()));
            }
        });
    }
}
