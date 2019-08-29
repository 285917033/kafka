package com.wbd.kafka.producer.serialize;

import java.io.Serializable;

public class Salary implements Serializable {

	private String id;
	
	private String salary;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSalary() {
		return salary;
	}

	public void setSalary(String salary) {
		this.salary = salary;
	}
	
	
	
}
