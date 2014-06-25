/*
 Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package test.java.org.apache.gora.hazelcast.store;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
//import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;
@SuppressWarnings("all")
public class Pageview extends PersistentBase {
  
  /**
   * Variable holding the data bean schema.
   */
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"Pageview\",\"namespace\":\"org.apache.gora.tutorial.log.generated\",\"fields\":[{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"ip\",\"type\":\"string\"},{\"name\":\"httpMethod\",\"type\":\"string\"},{\"name\":\"httpStatusCode\",\"type\":\"int\"},{\"name\":\"responseSize\",\"type\":\"int\"},{\"name\":\"referrer\",\"type\":\"string\"},{\"name\":\"userAgent\",\"type\":\"string\"}]}");
  
  /**
   * Enum containing all data bean's fields.
   */
  public static enum Field {
    URL(0,"url"),
    TIMESTAMP(1,"timestamp"),
    IP(2,"ip"),
    HTTP_METHOD(3,"httpMethod"),
    HTTP_STATUS_CODE(4,"httpStatusCode"),
    RESPONSE_SIZE(5,"responseSize"),
    REFERRER(6,"referrer"),
    USER_AGENT(7,"userAgent"),
    ;
    
    /**
     * Field's index.
     */
    private int index;
    
    /**
     * Field's name.
     */
    private String name;
    
    /**
     * Field's constructor
     * @param index field's index.
     * @param name field's name.
     */
    Field(int index, String name) {this.index=index;this.name=name;}
    
    /**
     * Gets field's index.
     * @return int field's index.
     */
    public int getIndex() {return index;}
    
    /**
     * Gets field's name.
     * @return String field's name.
     */
    public String getName() {return name;}
    
    /**
     * Gets field's attributes to string.
     * @return String field's attributes to string.
     */
    public String toString() {return name;}
  };
    
    /**
     * Contains all field's names.
     */
  public static final String[] _ALL_FIELDS = {"url","timestamp","ip","httpMethod","httpStatusCode","responseSize","referrer","userAgent",};
  static {
    PersistentBase.registerFields(Pageview.class, _ALL_FIELDS);
  }
  private Utf8 url;
  private long timestamp;
  private Utf8 ip;
  private Utf8 httpMethod;
  private int httpStatusCode;
  private int responseSize;
  private Utf8 referrer;
  private Utf8 userAgent;
  
  /**
   * Default Constructor
   */
  public Pageview() {
    this(new StateManagerImpl());
  }
  
  /**
   * Constructor
   * @param stateManager for the data bean.
   */
  public Pageview(StateManager stateManager) {
    super(stateManager);
  }
  
  /**
   * Returns a new instance by using a state manager.
   * @param stateManager for the data bean.
   * @return Pageview created.
   */
  public Pageview newInstance(StateManager stateManager) {
    return new Pageview(stateManager);
  }
  
  /**
   * Returns the schema of the data bean.
   * @return Schema for the data bean.
   */
  public Schema getSchema() { return _SCHEMA; }
  
  /**
   * Gets a specific field.
   * @param field index of a field for the data bean.
   * @return Object representing a data bean's field.
   */
  public Object get(int _field) {
    switch (_field) {
    case 0: return url;
    case 1: return timestamp;
    case 2: return ip;
    case 3: return httpMethod;
    case 4: return httpStatusCode;
    case 5: return responseSize;
    case 6: return referrer;
    case 7: return userAgent;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  
  /**
   * Puts a value for a specific field.
   * @param field index of a field for the data bean.
   * @param value value of a field for the data bean.
   */
  @SuppressWarnings(value="unchecked")
  public void put(int _field, Object _value) {
    if(isFieldEqual(_field, _value)) return;
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:url = (Utf8)_value; break;
    case 1:timestamp = (Long)_value; break;
    case 2:ip = (Utf8)_value; break;
    case 3:httpMethod = (Utf8)_value; break;
    case 4:httpStatusCode = (Integer)_value; break;
    case 5:responseSize = (Integer)_value; break;
    case 6:referrer = (Utf8)_value; break;
    case 7:userAgent = (Utf8)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  
  /**
   * Gets the Url.
   * @return Utf8 representing Pageview Url.
   */
  public Utf8 getUrl() {
    return (Utf8) get(0);
  }
  
  /**
   * Sets the Url.
   * @param value containing Pageview Url.
   */
  public void setUrl(Utf8 value) {
    put(0, value);
  }
  
  /**
   * Gets the Timestamp.
   * @return long representing Pageview Timestamp.
   */
  public long getTimestamp() {
    return (Long) get(1);
  }
  
  /**
   * Sets the Timestamp.
   * @param value containing Pageview Timestamp.
   */
  public void setTimestamp(long value) {
    put(1, value);
  }
  
  /**
   * Gets the Ip.
   * @return Utf8 representing Pageview Ip.
   */
  public Utf8 getIp() {
    return (Utf8) get(2);
  }
  
  /**
   * Sets the Ip.
   * @param value containing Pageview Ip.
   */
  public void setIp(Utf8 value) {
    put(2, value);
  }
  
  /**
   * Gets the HttpMethod.
   * @return Utf8 representing Pageview HttpMethod.
   */
  public Utf8 getHttpMethod() {
    return (Utf8) get(3);
  }
  
  /**
   * Sets the HttpMethod.
   * @param value containing Pageview HttpMethod.
   */
  public void setHttpMethod(Utf8 value) {
    put(3, value);
  }
  
  /**
   * Gets the HttpStatusCode.
   * @return int representing Pageview HttpStatusCode.
   */
  public int getHttpStatusCode() {
    return (Integer) get(4);
  }
  
  /**
   * Sets the HttpStatusCode.
   * @param value containing Pageview HttpStatusCode.
   */
  public void setHttpStatusCode(int value) {
    put(4, value);
  }
  
  /**
   * Gets the ResponseSize.
   * @return int representing Pageview ResponseSize.
   */
  public int getResponseSize() {
    return (Integer) get(5);
  }
  
  /**
   * Sets the ResponseSize.
   * @param value containing Pageview ResponseSize.
   */
  public void setResponseSize(int value) {
    put(5, value);
  }
  
  /**
   * Gets the Referrer.
   * @return Utf8 representing Pageview Referrer.
   */
  public Utf8 getReferrer() {
    return (Utf8) get(6);
  }
  
  /**
   * Sets the Referrer.
   * @param value containing Pageview Referrer.
   */
  public void setReferrer(Utf8 value) {
    put(6, value);
  }
  
  /**
   * Gets the UserAgent.
   * @return Utf8 representing Pageview UserAgent.
   */
  public Utf8 getUserAgent() {
    return (Utf8) get(7);
  }
  
  /**
   * Sets the UserAgent.
   * @param value containing Pageview UserAgent.
   */
  public void setUserAgent(Utf8 value) {
    put(7, value);
  }
}
