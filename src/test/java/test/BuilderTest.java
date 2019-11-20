package test;

import com.alibaba.fastjson.JSONObject;

import dataobject.Student;

public class BuilderTest {

	public static void main(String[] args) {

		Student student = Student.builder().name("charlie").age(11).classNo("101").build();
		System.out.println(JSONObject.toJSONString(student));

		student.setAge(12);
		System.out.println(JSONObject.toJSONString(student));

	}
}
