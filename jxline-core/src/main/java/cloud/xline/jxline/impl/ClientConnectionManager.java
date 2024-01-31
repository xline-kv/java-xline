package cloud.xline.jxline.impl;

import cloud.xline.jxline.ClientBuilder;

import io.etcd.jetcd.ByteSequence;
import io.grpc.*;
import io.grpc.netty.NegotiationType;
import io.netty.channel.ChannelOption;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.grpc.VertxChannelBuilder;

import java.util.concurrent.*;
import java.util.function.BiConsumer;

final class ClientConnectionManager {
    private final Object lock;
    private final ClientBuilder builder;
    private final ExecutorService executorService;
    private volatile Vertx vertx;

    ClientConnectionManager(ClientBuilder builder) {
        this.lock = new Object();
        this.builder = builder;

        if (builder.executorService() == null) {
            ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();
            // default to daemon
            this.executorService =
                    Executors.newCachedThreadPool(
                            r -> {
                                Thread t = backingThreadFactory.newThread(r);
                                t.setDaemon(true);
                                t.setName("jxline-" + t.getName());
                                return t;
                            });
        } else {
            this.executorService = builder.executorService();
        }
    }

    ByteSequence getNamespace() {
        return builder.namespace();
    }

    ExecutorService getExecutorService() {
        return executorService;
    }

    ClientBuilder builder() {
        return builder;
    }

    void close() {
        synchronized (lock) {
            if (vertx != null) {
                vertx.close();
            }
        }

        if (builder.executorService() == null) {
            executorService.shutdownNow();
        }
    }

    ManagedChannelBuilder<?> defaultChannelBuilder() {
        return defaultChannelBuilder(builder.target());
    }

    @SuppressWarnings("rawtypes")
    ManagedChannelBuilder<?> defaultChannelBuilder(String target) {
        if (target == null) {
            throw new IllegalArgumentException("At least one endpoint should be provided");
        }

        final VertxChannelBuilder channelBuilder = VertxChannelBuilder.forTarget(vertx(), target);

        if (builder.authority() != null) {
            channelBuilder.overrideAuthority(builder.authority());
        }
        if (builder.maxInboundMessageSize() != null) {
            channelBuilder.maxInboundMessageSize(builder.maxInboundMessageSize());
        }
        if (builder.sslContext() != null) {
            channelBuilder.nettyBuilder().negotiationType(NegotiationType.TLS);
            channelBuilder.nettyBuilder().sslContext(builder.sslContext());
        } else {
            channelBuilder.nettyBuilder().negotiationType(NegotiationType.PLAINTEXT);
        }

        if (builder.keepaliveTime() != null) {
            channelBuilder.keepAliveTime(builder.keepaliveTime().toMillis(), TimeUnit.MILLISECONDS);
        }
        if (builder.keepaliveTimeout() != null) {
            channelBuilder.keepAliveTimeout(
                    builder.keepaliveTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }
        if (builder.keepaliveWithoutCalls() != null) {
            channelBuilder.keepAliveWithoutCalls(builder.keepaliveWithoutCalls());
        }
        if (builder.connectTimeout() != null) {
            channelBuilder
                    .nettyBuilder()
                    .withOption(
                            ChannelOption.CONNECT_TIMEOUT_MILLIS,
                            (int) builder.connectTimeout().toMillis());
        }

        if (builder.loadBalancerPolicy() != null) {
            channelBuilder.defaultLoadBalancingPolicy(builder.loadBalancerPolicy());
        } else {
            channelBuilder.defaultLoadBalancingPolicy("pick_first");
        }

        if (builder.headers() != null) {
            channelBuilder.intercept(
                    new ClientInterceptor() {
                        @Override
                        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                MethodDescriptor<ReqT, RespT> method,
                                CallOptions callOptions,
                                Channel next) {

                            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                                    next.newCall(method, callOptions)) {
                                @Override
                                public void start(
                                        Listener<RespT> responseListener, Metadata headers) {
                                    builder.headers()
                                            .forEach(
                                                    (BiConsumer<Metadata.Key, Object>)
                                                            headers::put);
                                    super.start(responseListener, headers);
                                }
                            };
                        }
                    });
        }

        if (builder.interceptors() != null) {
            channelBuilder.intercept(builder.interceptors());
        }

        return channelBuilder;
    }

    Vertx vertx() {
        if (this.vertx == null) {
            synchronized (this.lock) {
                if (this.vertx == null) {
                    this.vertx = Vertx.vertx(new VertxOptions().setUseDaemonThread(true));
                }
            }
        }

        return this.vertx;
    }
}
