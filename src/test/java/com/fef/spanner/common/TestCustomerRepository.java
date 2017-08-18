package com.fef.spanner.common;


import com.fef.spanner.repository.CustomerRepository;
import com.google.cloud.Date;
import com.google.cloud.spanner.Key;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestCustomerRepository {

  private static CustomerRepository customerRepository;

  @BeforeClass
  public static void setUp(){
    DBFactory.Config config = new DBFactory.Config("fef-spanner","fef_db");
    customerRepository = new CustomerRepository("customer",config);
  }

  @Test
  public void testSave(){
    List<Map<String,Object>> list = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      Map<String,Object> map = new HashMap<>();
      map.put("id",Long.valueOf(i+1));
      map.put("customer_name","CUSTOMER"+(i+1));
      map.put("customer_status","Y");
      map.put("date_created", Date.parseDate(new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date())));
      list.add(map);
    }
    customerRepository.save(list);
  }

  @Test
  public void testSaveOrUpdate(){
    List<Map<String,Object>> list = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      Map<String,Object> map = new HashMap<>();
      map.put("id",Long.valueOf(i+1));
      map.put("customer_name","CUSTOMER"+(i+1));
      map.put("customer_status","Y");
      map.put("date_created", Date.parseDate(new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date())));
      list.add(map);
    }
    customerRepository.saveOrUpdate(list);
  }

  @Test
  public void testQuery(){
    List<Object[]> result = customerRepository.query("SELECT * FROM CUSTOMER");
    for (Object[] objects: result) {
      for (int i = 0; i < objects.length; i++) {
        System.out.println(objects[i]);  
      }
    }
  }

  @Test
  public void testDelete(){
    Key key = Key.newBuilder().append(1L).build();
    customerRepository.delete(key);
  }
}
