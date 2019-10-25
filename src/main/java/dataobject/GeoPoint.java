package dataobject;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GeoPoint {

	private Double lat;

	private Double lon;

}
