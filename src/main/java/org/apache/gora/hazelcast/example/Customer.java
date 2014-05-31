package main.java.org.apache.gora.hazelcast.example;

import java.io.Serializable;

public class Customer implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String name;
	private String country;
	private int age;
	
	public Customer(String name, String country,int age){
		this.name=name;
		this.country=country;
		this.age=age;
	}
	
	public String getName(){
		return name;
	}
	
	public String getCountry(){
		return country;
	}
	
	public int getAge(){
		return age;
	}

}
