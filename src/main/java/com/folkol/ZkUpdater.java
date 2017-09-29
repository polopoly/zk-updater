package com.folkol;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

public class ZkUpdater
{
    private static ZkClient zkClient;
    private static String cbHost;
    private static String zkHost;
    private static String bucket;
    private static String passwd;
    private static String dcpClientName;

    public static void main(String[] args) throws Exception
    {
        if (args.length != 5) {
            System.out.println("usage: java ZkUpdater cbHost zkHost bucketname bucketpasswd dcpClientName");
            System.out.println("\tExample: java -jar target/zk-updater-jar-with-dependencies.jar localhost " +
                                   "localhost cmbucket cmpasswd couchbase-kafka-connector2");
            System.exit(1);
        }

        cbHost = args[0];
        zkHost = args[1];
        bucket = args[2];
        passwd = args[3];
        dcpClientName = args[4];

        zkClient = new ZkClient(zkHost, 4000, 6000, ZKStringSerializer$.MODULE$);

        final Client client = Client.configure()
                                    .hostnames(cbHost)
                                    .bucket(bucket)
                                    .password(passwd)
                                    .build();
        client.controlEventHandler((channelFlowController, byteBuf) -> byteBuf.release());
        client.dataEventHandler((channelFlowController, byteBuf) -> byteBuf.release());

        client.connect().await();
        client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();
        SessionState sessionState = client.sessionState();

        client.failoverLogs()
              .toBlocking()
              .forEach(buffer -> {
                  if (DcpFailoverLogResponse.is(buffer)) {
                      System.out.println("Got failover log entry: " + DcpFailoverLogResponse.toString(buffer));

                      short partition = DcpFailoverLogResponse.vbucket(buffer);
                      long vid = DcpFailoverLogResponse.vbuuidEntry(buffer, 0);
                      PartitionState partitionState = sessionState.get(partition);
                      long seqno = partitionState.getStartSeqno();
                      writeState(partition, vid, seqno);
                  } else {
                      System.out.println("Expected DcpFailoverLog, got: " + buffer);
                  }

              });

        client.disconnect().await();
        zkClient.close();
    }

    private static String pathForState(final short partition) {
        return String.format("/%s/%s/%d", dcpClientName, bucket, partition);
    }

    private static void writeState(short partition, long vid, long seqno) {
        zkClient.createPersistent(pathForState(partition), true);
        String json = String.format("{\"vbucketUUID\": %d, \"sequenceNumber\": %d}",
                                    vid,
                                    seqno);

        String path = pathForState(partition);
        System.out.printf("Updating state %s -> %s%n", path, json);
        zkClient.writeData(path, json);
    }
}
