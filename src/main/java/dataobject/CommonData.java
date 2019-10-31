package dataobject;

import java.util.Date;
import java.util.List;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CommonData {

	private String code;

	private String name;

	private String title;

	private String desc;

	private String mark;

	private Integer number;

	private Integer cat;

	private Date time;

	private GeoPoint location;

	private List<String> list;

}
