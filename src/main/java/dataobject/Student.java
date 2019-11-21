package dataobject;

import elasticsearch.common.BaseDocument;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Student extends BaseDocument {

	private String name;

	private Integer age;

	private String classNo;
}
