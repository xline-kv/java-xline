package cloud.xline.jxline.exceptions;

import com.curp.protobuf.CurpError;
import com.google.protobuf.Empty;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;

import javax.annotation.Nullable;

public class CurpException extends RuntimeException {
    private static final CurpError DEFAULT_CURP_ERROR =
            CurpError.newBuilder().setRpcTransport(Empty.newBuilder().build()).build();

    private static final Metadata.Key<CurpError> STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin", ProtoUtils.metadataMarshaller(DEFAULT_CURP_ERROR));

    /**
     * The error details.
     */
    private final CurpError error;

    public CurpException(CurpError error) {
        this.error = error;
    }

    public enum Priority {
        LOW(1),
        HIGH(2);

        private final int value;

        Priority(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    public Priority priority() {
        if (this.error.hasDuplicated()
                || this.error.hasShuttingDown()
                || this.error.hasInvalidConfig()
                || this.error.hasNodeAlreadyExists()
                || this.error.hasNodeNotExists()
                || this.error.hasLearnerNotCatchUp()
                || this.error.hasExpiredClientId()
                || this.error.hasRedirect()) {
            return Priority.HIGH;
        }
        if (this.error.hasRpcTransport()
                || this.error.hasInternal()
                || this.error.hasKeyConflict()
                || this.error.hasLeaderTransfer()) {
            return Priority.LOW;
        }
        throw new RuntimeException("unknown curp error");
    }

    public boolean shouldAbortFastRound() {
        if (this.error.hasDuplicated()
                || this.error.hasShuttingDown()
                || this.error.hasInvalidConfig()
                || this.error.hasNodeAlreadyExists()
                || this.error.hasNodeNotExists()
                || this.error.hasLearnerNotCatchUp()
                || this.error.hasExpiredClientId()
                || this.error.hasRedirect()) {
            return true;
        }
        return false;
    }

    public boolean shouldAbortSlowRound() {
        if (this.error.hasShuttingDown()
                || this.error.hasInvalidConfig()
                || this.error.hasNodeAlreadyExists()
                || this.error.hasNodeNotExists()
                || this.error.hasLearnerNotCatchUp()
                || this.error.hasExpiredClientId()
                || this.error.hasRedirect()
                || this.error.hasWrongClusterVersion()) {
            return true;
        }
        return false;
    }

    public static CurpException toCurpException(Throwable throwable) {
        if (throwable instanceof CurpException) {
            return (CurpException) throwable;
        }
        return toCurpException(
                Status.fromThrowable(throwable), Status.trailersFromThrowable(throwable));
    }

    public static CurpException toCurpException(Status status, @Nullable Metadata trailers) {
        if (status.getCode() == Status.Code.UNAVAILABLE
                || status.getCode() == Status.Code.UNKNOWN
                || trailers == null) {
            return new CurpException(DEFAULT_CURP_ERROR);
        }
        CurpError curpError = trailers.get(STATUS_DETAILS_KEY);
        return new CurpException(curpError);
    }

    @Override
    public String toString() {
        return "CurpError(" + error.toString().replace("\n", "") + ")";
    }
}
