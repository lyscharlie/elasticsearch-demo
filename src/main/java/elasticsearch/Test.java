package elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

public class Test {

	public static void main(String[] args) {

		try {
			RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

			// 1、创建 创建索引request 参数：索引名test
			CreateIndexRequest request = new CreateIndexRequest("test");

			// 2、设置索引的settings
			request.settings(Settings.builder().put("index.number_of_shards", 3) // 分片数
					.put("index.number_of_replicas", 2) // 副本数
					.put("analysis.analyzer.default.tokenizer", "ik_smart") // 默认分词器
			);

			// 3、设置索引的mappings
			request.mapping("_doc", "  {\n" + "    \"_doc\": {\n" + "      \"properties\": {\n" + "        \"message\": {\n" + "          \"type\": \"text\"\n" + "        }\n"
					+ "      }\n" + "    }\n" + "  }", XContentType.JSON);

			// 4、 设置索引的别名
			request.alias(new Alias("mmm"));

			// 5、 发送请求
			// 5.1 同步方式发送请求
			CreateIndexResponse createIndexResponse = client.indices().create(request);

			// 6、处理响应
			boolean acknowledged = createIndexResponse.isAcknowledged();
			boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
			System.out.println("acknowledged = " + acknowledged);
			System.out.println("shardsAcknowledged = " + shardsAcknowledged);

			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
