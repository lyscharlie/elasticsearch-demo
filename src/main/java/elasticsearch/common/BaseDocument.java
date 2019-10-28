package elasticsearch.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BaseDocument {

	private String id;

	private Object object;

}
