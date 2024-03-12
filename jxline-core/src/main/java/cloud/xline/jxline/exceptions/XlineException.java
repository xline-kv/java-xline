package cloud.xline.jxline.exceptions;

import com.xline.protobuf.ExecuteError;
import io.etcd.jetcd.common.exception.ErrorCode;

import javax.annotation.Nullable;

public class XlineException extends RuntimeException {

    /**
     * {@link ExecuteError} when send invalid request to xline servers, maybe null when there is
     * some unknown exception happens in the runtime
     */
    private ExecuteError error;

    /**
     * {@link ErrorCode} when send invalid request to xline servers, it was mapped into etcd's
     * format on xline servers, maybe null when the client send request directly to curp server
     */
    private ErrorCode code;

    XlineException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public XlineException(ExecuteError error) {
        this.error = error;
    }

    public boolean hasError() {
        return error != null;
    }

    public boolean hasErrorCode() {
        return code != null;
    }

    @Nullable
    public ExecuteError getError() {
        return error;
    }

    @Nullable
    public ErrorCode getCode() {
        return code;
    }

    public static XlineException toXlineException(Throwable throwable) {
        return toXlineException(ErrorCode.INTERNAL, throwable.getMessage(), throwable);
    }

    public static XlineException toXlineException(ErrorCode code, Throwable throwable) {
        return toXlineException(code, throwable.getMessage(), throwable);
    }

    public static XlineException toXlineException(
            ErrorCode code, String message, Throwable throwable) {
        if (throwable instanceof XlineException) {
            return (XlineException) throwable;
        }
        return new XlineException(code, message, throwable);
    }
}
