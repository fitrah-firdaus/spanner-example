package com.fef.spanner.repository;

import com.fef.spanner.common.CrudRepository;
import com.fef.spanner.common.DBFactory;

public class CustomerRepository extends CrudRepository {
  public CustomerRepository(String tableName, DBFactory.Config config) {
    super(tableName, config);
  }
}
