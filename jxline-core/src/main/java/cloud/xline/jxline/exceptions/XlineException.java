package cloud.xline.jxline.exceptions;

import com.xline.protobuf.ExecuteError;

import javax.annotation.Nullable;

public class XlineException extends RuntimeException {

    /**
     * {@link ExecuteError} when send invalid request to xline servers, maybe null when there is
     * some unknown exception happens in the runtime
     */
    private ExecuteError error;

    private XlineException(Throwable cause) {
        super(cause);
    }

    public XlineException(ExecuteError error) {
        this.error = error;
    }

    public boolean hasError() {
        return error != null;
    }

    @Nullable
    public ExecuteError getError() {
        return error;
    }

    public static XlineException toXlineException(Throwable throwable) {
        if (throwable instanceof XlineException) {
            return (XlineException) throwable;
        }
        return new XlineException(throwable);
    }
}
