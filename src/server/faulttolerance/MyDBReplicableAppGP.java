package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxospackets.*;
import edu.umass.cs.nio.interfaces.*;
import edu.umass.cs.reconfiguration.reconfigurationutils.*;
import org.json.*;
import java.io.*;
import java.util.*;

public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    private Cluster cluster;
    private Session session;
    private String keyspace;
    private static final String TABLE = "grade";

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0) throw new IllegalArgumentException();
        keyspace = args[0];
        String host = "localhost";
        if (args.length > 1 && args[1] != null) host = args[1];
        cluster = Cluster.builder().addContactPoint(host).build();
        session = cluster.connect(keyspace);
    }

    public boolean execute(Request request, boolean doNotReplyToClient) {
        try {
            String command = extractCommand(request);
            if (command == null || command.trim().isEmpty()) return false;
            
            session.execute(command);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean execute(Request request) {
        return execute(request, false);
    }

    /**
     * Helper to get the string command (CQL) from the generic Request object.
     */
    private String extractCommand(Request request) {
        if (request instanceof RequestPacket) return ((RequestPacket) request).requestValue;
        return request != null ? request.toString() : null;
    }



    public String checkpoint(String name) {
        try {
            ResultSet resultSet = session.execute("SELECT * FROM " + keyspace + "." + TABLE + ";");
            JSONObject json = new JSONObject();
            JSONArray rowsArray = new JSONArray();
            
            for (Row row : resultSet) {
                JSONObject obj = new JSONObject();
                obj.put("id", row.getInt("id"));
                List<Integer> list = row.getList("events", Integer.class);
                JSONArray events = new JSONArray();
                if (list != null)
                    for (Integer i : list) events.put(i);
                obj.put("events", events);
                rowsArray.put(obj);
            }
            json.put("name", name);
            json.put("rows", rowsArray);
            return json.toString();
        } catch (Exception e) {
            return null;
        }
    }

    public boolean restore(String name, String state) {
        try {
            if (state == null || state.trim().isEmpty()) return true;
            JSONObject json = new JSONObject(state);
            JSONArray rowsArray = json.optJSONArray("rows");
            if (rowsArray == null || rowsArray.length() == 0) return true;
            
            // Clear stale state
            session.execute("TRUNCATE " + keyspace + "." + TABLE + ";");
            
            // Re-insert state from checkpoint
            for (int i = 0; i < rowsArray.length(); i++) {
                JSONObject obj = rowsArray.getJSONObject(i);
                StringBuilder eventsStr = new StringBuilder("[");
                JSONArray events = obj.getJSONArray("events");
                for (int m = 0; m < events.length(); m++) {
                    if (m > 0) eventsStr.append(",");
                    eventsStr.append(events.getInt(m));
                }
                eventsStr.append("]");
                session.execute("INSERT INTO " + keyspace + "." + TABLE + " (id, events) VALUES (" + obj.getInt("id") + ", " + eventsStr + ");");
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>();
    }
}