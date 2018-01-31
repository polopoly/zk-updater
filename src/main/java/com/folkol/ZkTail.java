package com.folkol;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.nio.charset.Charset;
import org.I0Itec.zkclient.ZkClient;


public class ZkTail
{
    private static ZkClient zkClient;
    private static String cbHost;
    private static String zkHost;
    private static String bucket;
    private static String passwd;
    private static String dcpClientName;

    public ZkTail() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("usage: zktail cbHost zkHost bucketname bucketpasswd dcpClientName");
            System.out.println("\tExample: java -cp zk-updater-jar-with-dependencies.jar com.folkol.ZkTail localhost localhost cmbucket cmpasswd couchbase-kafka-connector2");
            System.exit(1);
        }

        cbHost = args[0];
        zkHost = args[1];
        bucket = args[2];
        passwd = args[3];
        dcpClientName = args[4];
        zkClient = new ZkClient(zkHost, 4000, 6000);
        Client client = Client.configure().hostnames(new String[]{cbHost}).bucket(bucket).password(passwd).build();
        client.controlEventHandler((channelFlowController, byteBuf) -> {
            try {
                if (DcpSnapshotMarkerRequest.is(byteBuf)) {
                    System.out.println("Snapshot marker: " + DcpSnapshotMarkerRequest.toString(byteBuf));
                } else {
                    System.out.println("Got unknown message: " + byteBuf);
                }
            } finally {
                byteBuf.release();
            }

        });
        client.dataEventHandler((channelFlowController, byteBuf) -> {
            try {
                if (DcpMutationMessage.is(byteBuf)) {
                    System.out.printf("Mutation %d in vbucket %d: %s%n",
                                      DcpMutationMessage.bySeqno(byteBuf),
                                      DcpMutationMessage.partition(byteBuf),
                                      DcpMutationMessage.key(byteBuf).toString(Charset.defaultCharset()));
                } else if (DcpDeletionMessage.is(byteBuf)) {
                    System.out.printf("Deletion %d in vbucket %d: %s%n",
                                      DcpDeletionMessage.bySeqno(byteBuf),
                                      DcpDeletionMessage.partition(byteBuf),
                                      DcpDeletionMessage.key(byteBuf).toString(Charset.defaultCharset()));
                } else if (DcpExpirationMessage.is(byteBuf)) {
                    System.out.printf("Expiration %d in vbucket %d: %s%n",
                                      DcpExpirationMessage.bySeqno(byteBuf),
                                      DcpExpirationMessage.partition(byteBuf),
                                      DcpExpirationMessage.key(byteBuf).toString(Charset.defaultCharset()));
                } else {
                    System.out.println("Got unknown message: " + MessageUtil.getKey(byteBuf));
                }
            } finally {
                byteBuf.release();
            }

        });
        client.connect().await();
        client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();
        SessionState sessionState = client.sessionState();
        client.failoverLogs(new Short[0]).toBlocking().forEach((buffer) -> {
            if (DcpFailoverLogResponse.is(buffer)) {
                short partition = DcpFailoverLogResponse.vbucket(buffer);
                PartitionState partitionState = sessionState.get(partition);
                long[] offset = readState(partition);
                if (offset[0] == partitionState.getLastUuid()) {
                    System.err.printf("Partition %d: seqno=%d%n", partition, offset[1]);
                    partitionState.setStartSeqno(offset[1]);
                    sessionState.set(partition, partitionState);
                } else {
                    System.err.printf("IGNORING: Partition %d: seqno=%d (uuid=%d)%n", partition, offset[1], offset[0]);
                }
            } else {
                System.err.println("Expected DcpFailoverLog, got: " + buffer);
            }

        });
        zkClient.close();
        client.startStreaming().await();
    }

    private static String pathForState(short partition) {
        return String.format("/%s/%s/%d", dcpClientName, bucket, partition);
    }

    private static long[] readState(short partition) {
        zkClient.createPersistent(pathForState(partition), true);
        String path = pathForState(partition);
        String data = zkClient.readData(path);
        if (data != null) {
            JsonObject obj = (new Gson()).fromJson(data, JsonObject.class);
            long seqNo = obj.get("sequenceNumber").getAsLong();
            long vbucketUUID = obj.get("vbucketUUID").getAsLong();
            return new long[]{vbucketUUID, seqNo};
        } else {
            System.out.printf("Missing from Zookeeper: %s%n", path);
            return new long[]{0L, 0L};
        }
    }
}
