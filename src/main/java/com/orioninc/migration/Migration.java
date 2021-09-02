/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orioninc.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author memin
 */
public class Migration {

    private static final String URL = "jdbc:postgresql://localhost:5432/hazelcast";
    private static final String USER = "hazelcast";
    private static final String PASSWORD = "hazelcast";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        List<Input> inputs = getQuery();
        List<Output> outputs = convert(inputs);
        int i = deleteData();
        System.out.println("rows affected = " + i);
        insertData(outputs);
    }

    public static int deleteData() {
        String SQL = "delete from hazelcast_json_metadata where service = 'onedotfivebridge' and unique_key != username";

        int affectedrows = 0;

        try (Connection conn = connect();
                PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            affectedrows = pstmt.executeUpdate();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return affectedrows;
    }

    public static List<Input> getQuery() {
        String SQL = "select * from hazelcast_json_metadata where service = 'onedotfivebridge' and unique_key != username";
        List<Input> inputs = new ArrayList<>();
        try (Connection conn = connect();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SQL)) {
            while (rs.next()) {
                Input i = new Input();
                i.setService(rs.getString("service"));
                i.setUniqueKey(rs.getString("unique_key"));
                i.setUsername(rs.getString("username"));
                i.setVersion(rs.getString("version"));
                String data = rs.getString("data");
                SubscriptionInfo subscriptionInfo = convertJson(data);
                i.setData(subscriptionInfo);
                inputs.add(i);
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }

        return inputs;
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    private static SubscriptionInfo convertJson(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            SubscriptionInfo retVal = objectMapper.readValue(json, SubscriptionInfo.class);
            return retVal;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    private static Set<SubscriptionInfo> convertJsonList(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Set<SubscriptionInfo> retVal = objectMapper.readValue(json, mapper.getTypeFactory().constructCollectionType(Set.class, SubscriptionInfo.class));
            return retVal;
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

    private static List<Output> convert(List<Input> inputs) {
        List<Output> outputs = new ArrayList<>();
        Map<String, Output> outputMap = new HashMap<>();
        inputs.forEach(input -> {
            if (outputMap.containsKey(input.getUsername())) {
                input.getData().setChannelId(input.getUniqueKey());
                outputMap.get(input.getUsername()).getData().add(input.getData());
            } else {
                Output o = new Output();
                o.setService(input.getService());
                o.setUniqueKey(input.getUsername());
                o.setUsername(input.getUsername());
                o.setVersion(input.getVersion());
                Set<SubscriptionInfo> infoSet = new HashSet<>();
                input.getData().setChannelId(input.getUniqueKey());
                infoSet.add(input.getData());
                o.setData(infoSet);
                outputMap.put(input.getUsername(), o);
            }
        });
        outputs.addAll(outputMap.values());
        return outputs;
    }

    private static void insertData(List<Output> outputs) {
        String INSERTSQL = "INSERT INTO hazelcast_json_metadata(service,unique_key,version,data,username,modified_date) "
                + "VALUES(?,?,?,?,?,?)";
        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(INSERTSQL);) {
            int count = 0;

            for (Output output : outputs) {
                Output existingData = selectById(output.getUniqueKey(), output.getVersion());
                if (existingData == null) {
                    try {
                        statement.setString(1, output.getService());
                        statement.setString(2, output.getUniqueKey());
                        statement.setString(3, output.getVersion());
                        statement.setString(4, mapper.writeValueAsString(output.getData()));
                        statement.setString(5, output.getUsername());
                        statement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

                        statement.addBatch();
                    } catch (Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                } else {
                    existingData.getData().addAll(output.getData());
                    updateData(existingData);
                }
                count++;
                // execute every 100 rows or less
                if (count % 100 == 0 || count == outputs.size()) {
                    statement.executeBatch();
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void updateData(Output existingData) {

        String UPDATESQL = "UPDATE hazelcast_json_metadata set data = ? where unique_key = ?";
        try (
                Connection conn = connect();
                PreparedStatement statement = conn.prepareStatement(UPDATESQL);) {
            try {
                statement.setString(1, existingData.getUniqueKey());
                statement.setString(2, mapper.writeValueAsString(existingData.getData()));
                statement.executeUpdate();
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static Output selectById(String id, String version) {
        String SQL = "select * from hazelcast_json_metadata where service = 'onedotfivebridge' and unique_key = '" + id + "' and version = '" + version +"'";
        try (Connection conn = connect();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SQL)) {
            while (rs.next()) {
                Output o = new Output();
                o.setService(rs.getString("service"));
                o.setUniqueKey(rs.getString("unique_key"));
                o.setUsername(rs.getString("username"));
                o.setVersion(rs.getString("version"));
                String data = rs.getString("data");
                Set<SubscriptionInfo> subscriptionInfos = convertJsonList(data);
                o.setData(subscriptionInfos);
                return o;
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            return null;
        }
        return null;
    }

}
