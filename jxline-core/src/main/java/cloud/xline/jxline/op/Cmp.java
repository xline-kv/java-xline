package cloud.xline.jxline.op;

import cloud.xline.jxline.Txn;
import com.google.protobuf.ByteString;
import com.xline.protobuf.Compare;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.support.Util;


/** The compare predicate in {@link Txn}. */
public class Cmp {

    public enum Op {
        EQUAL,
        GREATER,
        LESS,
        NOT_EQUAL
    }

    private final ByteString key;
    private final Op op;
    private final CmpTarget<?> target;

    public Cmp(ByteSequence key, Op compareOp, CmpTarget<?> target) {
        this.key = ByteString.copyFrom(key.getBytes());
        this.op = compareOp;
        this.target = target;
    }

    Compare toCompare(ByteSequence namespace) {
        Compare.Builder compareBuilder =
                Compare.newBuilder().setKey(Util.prefixNamespace(this.key, namespace));
        switch (this.op) {
            case EQUAL:
                compareBuilder.setResult(Compare.CompareResult.EQUAL);
                break;
            case GREATER:
                compareBuilder.setResult(Compare.CompareResult.GREATER);
                break;
            case LESS:
                compareBuilder.setResult(Compare.CompareResult.LESS);
                break;
            case NOT_EQUAL:
                compareBuilder.setResult(Compare.CompareResult.NOT_EQUAL);
                break;
            default:
                throw new IllegalArgumentException("Unexpected compare type (" + this.op + ")");
        }

        Compare.CompareTarget target = this.target.getTarget();
        Object value = this.target.getTargetValue();

        compareBuilder.setTarget(target);
        switch (target) {
            case VERSION:
                compareBuilder.setVersion((Long) value);
                break;
            case VALUE:
                compareBuilder.setValue((ByteString) value);
                break;
            case MOD:
                compareBuilder.setModRevision((Long) value);
                break;
            case CREATE:
                compareBuilder.setCreateRevision((Long) value);
                break;
            default:
                throw new IllegalArgumentException("Unexpected target type (" + target + ")");
        }

        return compareBuilder.build();
    }
}
