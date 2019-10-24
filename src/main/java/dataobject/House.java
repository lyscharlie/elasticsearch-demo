package dataobject;

import java.util.List;

import lombok.Data;

@Data
public class House {

	private String houseCode;

	private String title;

	private String communityName;

	private String address;

	private String roomNo;

	private Integer chamber;

	private Integer board;

	private Integer toilet;

	private Double rentPrice;

	private Double longitude;

	private Double latitude;

	private List<String> tags;

}
