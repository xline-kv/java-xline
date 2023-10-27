package client;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.xline.protobuf.CompactionRequest;
import com.xline.protobuf.Compare;
import com.xline.protobuf.DeleteRangeRequest;
import com.xline.protobuf.DeleteRangeResponse;
import com.xline.protobuf.PutRequest;
import com.xline.protobuf.PutResponse;
import com.xline.protobuf.RangeRequest;
import com.xline.protobuf.RangeResponse;
import com.xline.protobuf.RequestOp;
import com.xline.protobuf.ResponseOp;
import com.xline.protobuf.TxnRequest;
import com.xline.protobuf.TxnResponse;
import com.xline.protobuf.Compare.CompareResult;
import com.xline.protobuf.Compare.CompareTarget;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

@Nested
public class KvTest {
    static KvClient kvClient;

    @BeforeAll
    static void connect() {
        String[] curpMembers = { "172.20.0.3:2379", "172.20.0.4:2379", "172.20.0.5:2379" };

        ArrayList<ManagedChannel> channels = new ArrayList<ManagedChannel>();
        for (String curpMember : curpMembers) {
            ManagedChannel channel = Grpc.newChannelBuilder(curpMember, InsecureChannelCredentials.create())
                    .build();
            channels.add(channel);
        }

        kvClient = new Client(curpMembers).kvClient;
    }

    @Test
    void testPutShouldSuccessInNormalPath() {
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("put"))
                .setValue(ByteString.copyFromUtf8("123")).build());

        // overwrite with prev key
        PutResponse res = kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("put"))
                .setValue(ByteString.copyFromUtf8("456")).setPrevKv(true).build());
        assertEquals(ByteString.copyFromUtf8("put"), res.getPrevKv().getKey());
        assertEquals(ByteString.copyFromUtf8("123"), res.getPrevKv().getValue());

        // overwrite again with prev key
        res = kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("put"))
                .setValue(ByteString.copyFromUtf8("456")).setPrevKv(true).build());
        assertEquals(ByteString.copyFromUtf8("put"), res.getPrevKv().getKey());
        assertEquals(ByteString.copyFromUtf8("456"), res.getPrevKv().getValue());
    }

    @Test
    void testRangeShouldFetchesPreviouslyPutKeys() {
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("get10"))
                .setValue(ByteString.copyFromUtf8("10")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("get11"))
                .setValue(ByteString.copyFromUtf8("11")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("get20"))
                .setValue(ByteString.copyFromUtf8("20")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("get21"))
                .setValue(ByteString.copyFromUtf8("21")).build());

        // get key
        RangeResponse res = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("get11")).build());
        assertEquals(1, res.getCount());
        assertFalse(res.getMore());
        assertEquals(1, res.getKvsList().size());
        assertEquals(ByteString.copyFromUtf8("get11"), res.getKvs(0).getKey());
        assertEquals(ByteString.copyFromUtf8("11"), res.getKvs(0).getValue());
    }

    @Test
    void testDeleteShouldRemovePreviouslyPutKvs() {
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("del10"))
                .setValue(ByteString.copyFromUtf8("10")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("del11"))
                .setValue(ByteString.copyFromUtf8("11")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("del20"))
                .setValue(ByteString.copyFromUtf8("20")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("del21"))
                .setValue(ByteString.copyFromUtf8("21")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("del31"))
                .setValue(ByteString.copyFromUtf8("31")).build());
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("del32"))
                .setValue(ByteString.copyFromUtf8("32")).build());

        // delete key
        DeleteRangeResponse delRes = kvClient.delete(
                DeleteRangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("del11")).setPrevKv(true)
                        .build());
        assertEquals(1, delRes.getDeleted());
        assertEquals(ByteString.copyFromUtf8("del11"), delRes.getPrevKvs(0).getKey());
        assertEquals(ByteString.copyFromUtf8("11"), delRes.getPrevKvs(0).getValue());

        RangeResponse getRes = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("del11"))
                        .setCountOnly(true).build());
        assertEquals(0, getRes.getCount());

        // delete a range of keys
        delRes = kvClient.delete(
                DeleteRangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("del11"))
                        .setRangeEnd(ByteString.copyFromUtf8("del22")).setPrevKv(true).build());
        assertEquals(2, delRes.getDeleted());
        assertEquals(ByteString.copyFromUtf8("del20"), delRes.getPrevKvs(0).getKey());
        assertEquals(ByteString.copyFromUtf8("20"), delRes.getPrevKvs(0).getValue());
        assertEquals(ByteString.copyFromUtf8("del21"), delRes.getPrevKvs(1).getKey());
        assertEquals(ByteString.copyFromUtf8("21"), delRes.getPrevKvs(1).getValue());

        getRes = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("del11"))
                        .setRangeEnd(ByteString.copyFromUtf8("del22")).setCountOnly(true)
                        .build());
        assertEquals(0, getRes.getCount());
    }

    @Test
    void testTxnShouldExecuteAsExpected() {
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("txn01"))
                .setValue(ByteString.copyFromUtf8("01")).build());

        // transaction 1
        Compare cmp1 = Compare.newBuilder().setKey(ByteString.copyFromUtf8("txn01"))
                .setValue(ByteString.copyFromUtf8("01")).setTarget(CompareTarget.VALUE)
                .setResult(CompareResult.EQUAL).build();
        RequestOp succ1 = RequestOp.newBuilder()
                .setRequestPut(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("txn01"))
                        .setValue(ByteString.copyFromUtf8("02")).setPrevKv(true).build())
                .build();
        RequestOp fail1 = RequestOp.newBuilder().setRequestRange(
                RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("txn01")).build()).build();
        TxnResponse txnRes = kvClient.txn(
                TxnRequest.newBuilder().addCompare(cmp1).addSuccess(succ1).addFailure(fail1).build());
        assertTrue(txnRes.getSucceeded());
        List<ResponseOp> opRes = txnRes.getResponsesList();
        assertEquals(opRes.size(), 1);
        PutResponse putRes = opRes.get(0).getResponsePut();
        assertEquals(putRes.getPrevKv().getValue(), ByteString.copyFromUtf8("01"));

        RangeResponse getRes = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("txn01")).build());
        assertEquals(getRes.getKvs(0).getKey(), ByteString.copyFromUtf8("txn01"));
        assertEquals(getRes.getKvs(0).getValue(), ByteString.copyFromUtf8("02"));

        // transaction 2
        Compare cmp2 = Compare.newBuilder().setKey(ByteString.copyFromUtf8("txn01"))
                .setValue(ByteString.copyFromUtf8("01")).setTarget(CompareTarget.VALUE)
                .setResult(CompareResult.EQUAL).build();
        RequestOp succ2 = RequestOp.newBuilder()
                .setRequestPut(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("txn01"))
                        .setValue(ByteString.copyFromUtf8("02")).setPrevKv(true).build())
                .build();
        RequestOp fail2 = RequestOp.newBuilder().setRequestRange(
                RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("txn01")).build()).build();
        txnRes = kvClient.txn(
                TxnRequest.newBuilder().addCompare(cmp2).addSuccess(succ2).addFailure(fail2).build());
        assertFalse(txnRes.getSucceeded());
        opRes = txnRes.getResponsesList();
        assertEquals(opRes.size(), 1);
        getRes = opRes.get(0).getResponseRange();
        assertEquals(getRes.getKvs(0).getValue(), ByteString.copyFromUtf8("02"));
    }

    @Test
    void testCompactShouldRemovePreviousRevision() {
        kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("compact"))
                .setValue(ByteString.copyFromUtf8("0")).build());
        PutResponse putRes = kvClient.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8("compact"))
                .setValue(ByteString.copyFromUtf8("1")).build());
        long rev = putRes.getHeader().getRevision();

        // before compacting
        RangeResponse rev0res = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("compact"))
                        .setRevision(rev - 1).build());
        assertEquals(ByteString.copyFromUtf8("0"), rev0res.getKvs(0).getValue());

        RangeResponse rev1res = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("compact"))
                        .setRevision(rev).build());
        assertEquals(ByteString.copyFromUtf8("1"), rev1res.getKvs(0).getValue());

        kvClient.compact(CompactionRequest.newBuilder().setRevision(rev).build());

        // after compacting
        try {
            kvClient
                    .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("compact"))
                            .setRevision(rev - 1)
                            .build());
            assertTrue(false);
        } catch (Exception e) {
        }
        // assertEquals(ByteString.copyFromUtf8("0"), rev0res.getKvs(0).getValue());

        RangeResponse getRes = kvClient
                .get(RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8("compact"))
                        .setRevision(rev).build());
        assertEquals(ByteString.copyFromUtf8("1"), getRes.getKvs(0).getValue());
    }
}
