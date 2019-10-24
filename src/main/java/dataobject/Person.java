package dataobject;

import java.util.Date;

import lombok.Data;

@Data
public class Person {

	private String id;

	private String name;

	private int sex;

	private int age;

	private Date birthday;

	private String mark;

}
