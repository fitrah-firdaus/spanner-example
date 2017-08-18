package com.fef.spanner.common;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class CrudRepository {
  private final String tableName;
  private final DBFactory.Config config;

  protected CrudRepository(String tableName, DBFactory.Config config) {
    this.tableName = tableName;
    this.config = config;
  }

  public Timestamp save(List<Map<String, Object>> dataList) {
    List<Mutation> mutationList = new ArrayList<>();
    for (Map<String, Object> map : dataList) {
      Mutation.WriteBuilder writeBuilder = Mutation.newInsertBuilder(tableName);
      buildingWriteBuilder(writeBuilder, map);
      mutationList.add(writeBuilder.build());
    }
    return DBFactory.getDatabaseClient(config).write(mutationList);
  }

  public Timestamp saveOrUpdate(List<Map<String, Object>> dataList) {
    List<Mutation> mutationList = new ArrayList<>();
    for (Map<String, Object> map : dataList) {
      Mutation.WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(tableName);
      buildingWriteBuilder(writeBuilder, map);
      mutationList.add(writeBuilder.build());
    }
    return DBFactory.getDatabaseClient(config).write(mutationList);
  }

  public List<Object[]> query(String query) {
    ResultSet rs = DBFactory.getDatabaseClient(config).singleUse().executeQuery(Statement.of(query));
    List<Object[]> result = new LinkedList<>();
    while (rs.next()) {
      Object[] obj = new Object[rs.getColumnCount()];
      for (int i = 0; i < rs.getColumnCount(); i++) {
        if (!rs.isNull(i)) {
          if (rs.getColumnType(i) == Type.bool()) {
            obj[i] = rs.getBoolean(i);
          } else if (rs.getColumnType(i) == Type.int64()) {
            obj[i] = rs.getLong(i);
          } else if (rs.getColumnType(i) == Type.float64()) {
            obj[i] = rs.getDouble(i);
          } else if (rs.getColumnType(i) == Type.string()) {
            obj[i] = rs.getString(i);
          } else if (rs.getColumnType(i) == Type.bytes()) {
            obj[i] = rs.getBytes(i);
          } else if (rs.getColumnType(i) == Type.timestamp()) {
            obj[i] = rs.getTimestamp(i);
          } else if (rs.getColumnType(i) == Type.date()) {
            obj[i] = rs.getDate(i);
          }
        }
      }
      result.add(obj);
    }
    return result;
  }

  public void delete(Key key){
    List<Mutation> mutationList = new ArrayList<>();
    Mutation mutation = Mutation.delete(tableName,key);
    mutationList.add(mutation);
    DBFactory.getDatabaseClient(config).write(mutationList);
  }

  private void buildingWriteBuilder(Mutation.WriteBuilder writeBuilder, Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof Boolean) {
        writeBuilder.set(entry.getKey()).to((Boolean) entry.getValue());
      } else if (entry.getValue() instanceof Long) {
        writeBuilder.set(entry.getKey()).to((Long) entry.getValue());
      } else if (entry.getValue() instanceof Double) {
        writeBuilder.set(entry.getKey()).to((Double) entry.getValue());
      } else if (entry.getValue() instanceof String) {
        writeBuilder.set(entry.getKey()).to((String) entry.getValue());
      } else if (entry.getValue() instanceof ByteArray) {
        writeBuilder.set(entry.getKey()).to((ByteArray) entry.getValue());
      } else if (entry.getValue() instanceof Timestamp) {
        writeBuilder.set(entry.getKey()).to((Timestamp) entry.getValue());
      } else if (entry.getValue() instanceof Date) {
        writeBuilder.set(entry.getKey()).to((Date) entry.getValue());
      }
    }
  }
}
