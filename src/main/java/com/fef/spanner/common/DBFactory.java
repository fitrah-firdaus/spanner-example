package com.fef.spanner.common;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

public class DBFactory {
  private static DBFactory instance = null;
  private static SpannerOptions spannerOptions = null;
  private static Spanner spanner = null;
  private DBFactory() {
  }

  private static DBFactory getInstance(){
    if(instance == null){
      instance = new DBFactory();
      spannerOptions = SpannerOptions.newBuilder().build();
      spanner = spannerOptions.getService();
    }
    return instance;
  }

  public static DatabaseClient getDatabaseClient(Config config){
    DBFactory dbFactory = getInstance();
    return spanner.getDatabaseClient(DatabaseId.of(spannerOptions.getProjectId(),config.instance,config.databaseName));
  }

  public static class Config {
    private String instance;
    private String databaseName;

    public Config(String instance, String databaseName) {
      this.instance = instance;
      this.databaseName = databaseName;
    }
  }
}
