package dataobject;

import java.util.Date;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CommonData {

	private String code;

	private String name;

	private String desc;

	private String mark;

	private Integer number;

	private Date time;

	private GeoPoint location;

}
