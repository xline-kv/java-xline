package cloud.xline.jxline.impl;

import io.vertx.core.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

abstract class Impl {
    private final Logger logger;
    private final ClientConnectionManager connectionManager;

    protected Impl(ClientConnectionManager connectionManager) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.connectionManager = connectionManager;
    }

    protected ClientConnectionManager connectionManager() {
        return this.connectionManager;
    }

    /**
     * Returns the logger for this class.
     *
     * @return the logger
     */
    protected Logger logger() {
        return this.logger;
    }

    /**
     * Converts Future of Type S to CompletableFuture of Type T.
     *
     * @param sourceFuture the Future to wrap
     * @param resultConvert the result converter
     * @return a {@link CompletableFuture} wrapping the given {@link Future}
     */
    protected <S, T> CompletableFuture<T> completable(
            Future<S> sourceFuture, Function<S, T> resultConvert) {
        return completable(sourceFuture, resultConvert, throwable -> throwable);
    }

    /**
     * Converts Future of Type S to CompletableFuture of Type T and exceptions.
     *
     * @param sourceFuture the Future to wrap
     * @param resultConvert the result converter
     * @param exceptionConverter the exception mapper
     * @return a {@link CompletableFuture} wrapping the given {@link Future}
     */
    protected <S, T> CompletableFuture<T> completable(
            Future<S> sourceFuture,
            Function<S, T> resultConvert,
            Function<Throwable, Throwable> exceptionConverter) {

        return completable(
                sourceFuture.compose(
                        r -> Future.succeededFuture(resultConvert.apply(r)),
                        e -> Future.failedFuture(exceptionConverter.apply(e))));
    }

    /**
     * Converts Future to CompletableFuture.
     *
     * @param sourceFuture the Future to wrap
     * @return a {@link CompletableFuture} wrapping the given {@link Future}
     */
    protected <S> CompletableFuture<S> completable(Future<S> sourceFuture) {
        return sourceFuture.toCompletionStage().toCompletableFuture();
    }

    // TODO: implement retry policy
}
