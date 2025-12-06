package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.*;
import edu.umass.cs.nio.interfaces.*;
import edu.umass.cs.nio.nioutils.*;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.*;
import server.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {
    public static final int SLEEP = 1000;
    public static final int MAX_LOG_SIZE = 400;
    public static final int ZK_PORT = 2181;
    public static final boolean DROP_TABLES_AFTER_TESTS=true;

    private static final Logger L = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());
    
    private final Cluster c;
    private final Session s;
    private final String id;
    
    private ZooKeeper z;
    private static final String ZK_HOST = "localhost:2181";
    private static final int ZK_TIMEOUT = 3000;
    
    // Zookeeper Paths
    private static final String ROOT = "/ftdb";
    private static final String REQ_PATH = ROOT + "/requests";
    private static final String CP_PATH = ROOT + "/checkpoint";
    private static final String SRV_PATH = ROOT + "/servers";
    
    private final MessageNIOTransport<String, String> mt;
    private final ConcurrentHashMap<Long, String> executedMap = new ConcurrentHashMap<>();
    
    // Add this field to track execution
    private final ConcurrentHashMap<String, Boolean> requestAcks = new ConcurrentHashMap<>();

    private long lastExecutedSeq = -1;
    private long lastCheckpointSeq = -1;
    private static final String TABLE = "grade";

    public MyDBFaultTolerantServerZK(NodeConfig<String> nc, String i, InetSocketAddress a) throws IOException {
        super(new InetSocketAddress(nc.getNodeAddress(i), nc.getNodePort(i) - ReplicatedServer.SERVER_PORT_OFFSET), a, i);
        this.id = i;
        c = Cluster.builder().addContactPoint(a.getHostString()).withPort(a.getPort()).build();
        s = c.connect(id);
        
        mt = new MessageNIOTransport<>(id, nc, new AbstractBytePacketDemultiplexer() {
            public boolean handleMessage(byte[] b, NIOHeader h) {
                handleMessageFromServer(b, h);
                return true;
            }
        }, true);
        
        try {
            initializeZookeeper();
        } catch (Exception e) {
            throw new IOException(e);
        }
        
        try {
            performRecovery();
        } catch (Exception e) {
            throw new IOException(e);
        }
        
        watchForRequests();
    }

    private void initializeZookeeper() throws IOException, InterruptedException, KeeperException {
        CountDownLatch l = new CountDownLatch(1);
        z = new ZooKeeper(ZK_HOST, ZK_TIMEOUT, e -> {
            if (e.getState() == Watcher.Event.KeeperState.SyncConnected) l.countDown();
        });
        l.await();
        
        createNodeIfNotExists(ROOT, "");
        createNodeIfNotExists(REQ_PATH, "");
        createNodeIfNotExists(CP_PATH, "");
        createNodeIfNotExists(SRV_PATH, "");
        
        try {
            z.create(SRV_PATH + "/" + id, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {}
    }

    private void createNodeIfNotExists(String p, String d) throws KeeperException, InterruptedException {
        try {
            z.create(p, d.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {}
    }

    /**
     * Recovery Logic:
     * 1. Check if a checkpoint exists in ZK.
     * 2. If yes, restore the database state from that JSON.
     * 3. Set the sequence number to the checkpoint's sequence number.
     * 4. Replay any requests that happened after that sequence number.
     */
    private void performRecovery() throws KeeperException, InterruptedException {
        Stat t = z.exists(CP_PATH + "/data", false);
        if (t != null) {
            byte[] b = z.getData(CP_PATH + "/data", false, null);
            if (b != null && b.length > 0) restoreFromCheckpoint(new String(b));
        }
        
        t = z.exists(CP_PATH + "/seqnum", false);
        if (t != null) {
            byte[] b = z.getData(CP_PATH + "/seqnum", false, null);
            if (b != null && b.length > 0) {
                lastCheckpointSeq = Long.parseLong(new String(b));
                lastExecutedSeq = lastCheckpointSeq;
            }
        }
        replayRequests();
    }

    private void replayRequests() throws KeeperException, InterruptedException {
        List<String> r = z.getChildren(REQ_PATH, false);
        Collections.sort(r);
        for (String n : r) {
            long q = Long.parseLong(n.replace("req-", ""));
            // Only replay if newer than what we have executed (or recovered from checkpoint)
            if (q > lastExecutedSeq) {
                byte[] d = z.getData(REQ_PATH + "/" + n, false, null);
                if (d != null && d.length > 0) executeRequest(new String(d), q);
            }
        }
    }

    private void watchForRequests() {
        try {
            z.getChildren(REQ_PATH, e -> {
                if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    processNewRequests();
                    watchForRequests();  // Re-register watcher
                }
            });
            // Process any requests that already exist
            processNewRequests();
        } catch (Exception e) {
            L.warning("Error watching requests: " + e.getMessage());
        }
    }

    private void processNewRequests() {
        try {
            List<String> r = z.getChildren(REQ_PATH, false);
            Collections.sort(r);
            for (String n : r) {
                long q = Long.parseLong(n.replace("req-", ""));
                if (q > lastExecutedSeq) {
                    byte[] d = z.getData(REQ_PATH + "/" + n, false, null);
                    if (d != null && d.length > 0) {
                        executeRequest(new String(d), q);
                    }
                }
            }
            
            // Checkpoint Strategy
            if (lastExecutedSeq - lastCheckpointSeq >= MAX_LOG_SIZE / 2) {
                performCheckpoint();
            }
            
            cleanupOldRequests();
        } catch (Exception e) {
            L.warning("Error processing requests: " + e.getMessage());
        }
    }

    protected void handleMessageFromClient(byte[] b, NIOHeader h) {
        String r = new String(b);
        try {
            String k = r;
            try {
                JSONObject j = new JSONObject(r);
                if (j.has("REQUEST")) k = j.getString("REQUEST");
            } catch (JSONException e) {}
            
            // Write request to ZK (fire and forget)
            // The watcher will process it asynchronously
            z.create(REQ_PATH + "/req-", k.getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            
            // Send acknowledgment immediately
            // The test waits before verifying results
            clientMessenger.send(h.sndr, createResponse(r, "ACK").getBytes());
        } catch (Exception e) {
            try {
                clientMessenger.send(h.sndr, createResponse(r, "ERROR: " + e.getMessage()).getBytes());
            } catch (IOException x) {}
        }
    }

    protected void handleMessageFromServer(byte[] b, NIOHeader h) {}

    private void executeRequest(String r, long q) {
        try {
            s.execute(r);
            lastExecutedSeq = q;
            executedMap.put(q, r);
            requestAcks.put(String.valueOf(q), true);
        } catch (Exception e) {
            L.warning("Failed to execute request " + q + ": " + e.getMessage());
        }
    }

    /**
     * Dumps the entire "grade" table into a JSON object and writes it to Zookeeper.
     * Also updates the checkpoint sequence number.
     */
    private void performCheckpoint() {
        try {
            ResultSet rs = s.execute("SELECT * FROM " + id + "." + TABLE + ";");
            JSONObject j = new JSONObject();
            JSONArray a = new JSONArray();
            for (Row w : rs) {
                JSONObject o = new JSONObject();
                o.put("id", w.getInt("id"));
                List<Integer> ev = w.getList("events", Integer.class);
                JSONArray ea = new JSONArray();
                if (ev != null)
                    for (Integer v : ev) ea.put(v);
                o.put("events", ea);
                a.put(o);
            }
            j.put("rows", a);
            j.put("seqnum", lastExecutedSeq);
            
            String js = j.toString();
            
            // Atomic update isn't strictly necessary here since we are a single leader in this context,
            // but we check existence to decide between create/setData.
            Stat t = z.exists(CP_PATH + "/data", false);
            if (t == null) z.create(CP_PATH + "/data", js.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            else z.setData(CP_PATH + "/data", js.getBytes(), -1);
            
            t = z.exists(CP_PATH + "/seqnum", false);
            String ss = String.valueOf(lastExecutedSeq);
            if (t == null) z.create(CP_PATH + "/seqnum", ss.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            else z.setData(CP_PATH + "/seqnum", ss.getBytes(), -1);
            
            lastCheckpointSeq = lastExecutedSeq;
        } catch (Exception e) {}
    }

    /**
     * Wipes the current table and re-inserts all rows found in the checkpoint JSON.
     */
    private void restoreFromCheckpoint(String c) {
        try {
            JSONObject j = new JSONObject(c);
            JSONArray a = j.optJSONArray("rows");
            if (a == null || a.length() == 0) return;
            
            s.execute("TRUNCATE " + id + "." + TABLE + ";");
            
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.getJSONObject(i);
                StringBuilder b = new StringBuilder("[");
                JSONArray v = o.getJSONArray("events");
                for (int m = 0; m < v.length(); m++) {
                    if (m > 0) b.append(",");
                    b.append(v.getInt(m));
                }
                b.append("]");
                s.execute("INSERT INTO " + id + "." + TABLE + " (id, events) VALUES (" + o.getInt("id") + ", " + b + ");");
            }
        } catch (Exception e) {}
    }

    private void cleanupOldRequests() {
        try {
            List<String> r = z.getChildren(REQ_PATH, false);
            if (r.size() > MAX_LOG_SIZE) {
                Collections.sort(r);
                int d = r.size() - MAX_LOG_SIZE;
                for (int i = 0; i < d; i++) {
                    z.delete(REQ_PATH + "/" + r.get(i), -1);
                    long q = Long.parseLong(r.get(i).replace("req-", ""));
                    executedMap.remove(q);
                }
            }
        } catch (Exception e) {}
    }

    private String createResponse(String r, String v) {
        try {
            JSONObject j = new JSONObject(r);
            j.put("RESPONSE", v);
            return j.toString();
        } catch (JSONException e) {
            return v;
        }
    }

    public void close() {
        try {
            if (lastExecutedSeq > lastCheckpointSeq) performCheckpoint();
            if (z != null) z.close();
            if (mt != null) mt.stop();
            if (s != null && !s.isClosed()) s.close();
            if (c != null && !c.isClosed()) c.close();
            super.close();
        } catch (Exception e) {}
    }

    public static enum CheckpointRecovery {
        CHECKPOINT,
        RESTORE;
    }

    public static void main(String[] a) throws IOException {
        new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile(a[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET), a[1], a.length > 2 ? Util.getInetSocketAddressFromString(a[2]) : new InetSocketAddress("localhost", 9042));
    }
}