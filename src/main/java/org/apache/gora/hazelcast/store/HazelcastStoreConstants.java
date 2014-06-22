package main.java.org.apache.gora.hazelcast.store;

/**
 * @author Ahsan Shamsudeen
 */
public class HazelcastStoreConstants {
	
	public HazelcastStoreConstants(){
		
	}

  /**
   * The mapping file to create the tables from
   */
  public static final String DEFAULT_MAPPING_FILE = "gora-hazelcast-mapping.xml";
  public static final String DEFAULT_CONFIGURATION_FILE = "hazelcast.xml";

  /**
   * The names of the properties in the gora.properties file.
   */
  
  public static final String STORE_NAME = "storename";
  public static final String PRIMARYKEY_TABLE_NAME = "primarykey_tablename";
  public static final String DEFAULT_STORE_NAME = "hazelcaststore";
  public static final String DEFAULT_PRIMARYKEY_TABLE_NAME = "PrimaryKeys";
  public static final String PROPERTIES_SEPARATOR = ",";
}
