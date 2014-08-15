package org.exoplatform.social.core.mongo.storage;

import java.lang.reflect.UndeclaredThrowableException;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import org.exoplatform.container.xml.InitParams;
import org.exoplatform.container.xml.ValueParam;
import org.picocontainer.Startable;

public class MongoStorage implements Startable {

  /** . */
  private DB db;
  
  /** . */
  private final String host;

  /** . */
  private final int port;
  
  private final String name;
  
  /**
   * Create a mongo store with the specified init params.
   *
   * @param params the init params
   */
  public MongoStorage(InitParams params) {

      //
      ValueParam hostParam = params.getValueParam("host");
      ValueParam portParam = params.getValueParam("port");
      ValueParam nameParam = params.getValueParam("name");
      //
      String host = hostParam != null ? hostParam.getValue().trim() : "localhost";
      int port = portParam != null ? Integer.parseInt(portParam.getValue().trim()) : 27017;
      String name = nameParam != null ? nameParam.getValue().trim() : "social";

      this.host = host;
      this.port = port;
      this.name = name;
  }

  /**
   * Create a mongo store with <code>localhost</code> host and <code>27017</code> port.
   */
  public MongoStorage() {
      this("localhost", 27017, "social");
  }
  
  /**
   * Create a mongo store with the specified connection parameters.
   *
   * @param host the host
   * @param port the port
   */
  public MongoStorage(String host, int port, String name) {
      this.host = host;
      this.port = port;
      this.name = name;
  }
  
  public DB getDB() {
    return db;
  }

  
  @Override
  public void start() {
    try {
      MongoClient mongo = new MongoClient(host, port);
      this.db = mongo.getDB(name);
      //DB admin = mongo.getDB("admin");
      //DBObject cmd = new BasicDBObject("shardCollection",new BasicDBObject()
                                       //.append("social.comment", new BasicDBObject("_id", "hashed")));
      //System.err.println("============>MongoStorage: "+admin.command(cmd));;
    } catch (MongoException e) {
      throw new UndeclaredThrowableException(e);
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e);
    }
  }

  @Override
  public void stop() {
    
  }

}
