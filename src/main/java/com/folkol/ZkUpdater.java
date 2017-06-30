package com.folkol;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.deps.io.netty.util.ReferenceCounted;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

public class ZkUpdater
{
    private static ZkClient zkClient;

    public static void main(String[] args) throws Exception
    {
        if (args.length != 4) {
            System.out.println("usage: java ZkUpdater cbHost zkHost bucketname bucketpasswd");
            System.exit(1);
        }

        String cbHost = args[0];
        String zkHost = args[1];
        String bucket = args[2];
        String passwd = args[3];

        zkClient = new ZkClient(zkHost, 4000, 6000, ZKStringSerializer$.MODULE$);

        final Client client = Client.configure()
                                    .hostnames(cbHost)
                                    .bucket(bucket)
                                    .password(passwd)
                                    .build();
        client.controlEventHandler(ReferenceCounted::release);
        client.dataEventHandler(event -> {
            System.out.println("Got event: " + event);
        });
        client.connect().await();


        client.failoverLogs()
              .toBlocking()
              .forEach(buffer -> {
                  if (DcpFailoverLogResponse.is(buffer)) {
                      System.out.println("Got failover log entry: " + DcpFailoverLogResponse.toString(buffer));

                      short partition = DcpFailoverLogResponse.vbucket(buffer);
                      long vid = DcpFailoverLogResponse.vbuuidEntry(buffer, 0);
                      long seqno = DcpFailoverLogResponse.seqnoEntry(buffer, 0);

                      writeState(partition, vid, seqno);
                  } else {
                      System.out.println("Expected DcpFailoverLog, got: " + buffer);
                  }

              });
        client.disconnect().await();
        zkClient.close();
    }

    private static String pathForState(final short partition) {
        return String.format("/couchbase-kafka-connector2/%s/%d", "cmbucket", partition);
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
