package elasticsearch.common;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchClientFactory {

	/**
	 * 初始化客户端
	 *
	 * @param scheme
	 * @param host
	 * @param port
	 * @return
	 */
	public static RestHighLevelClient initClient(String scheme, String host, int port) {
		return new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));
	}

	/**
	 * 创建索引
	 *
	 * @param index（index名必须全小写，否则报错）
	 * @param mappings
	 * @return
	 */
	public static boolean createIndex(RestHighLevelClient client, String index, String mappings) {
		try {
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
			if (StringUtils.isNotBlank(mappings)) {
				createIndexRequest.mapping(mappings, XContentType.JSON);
			}
			CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			if (createIndexResponse.isAcknowledged()) {
				log.info("创建索引成功");
			} else {
				log.info("创建索引失败");
			}
			return createIndexResponse.isAcknowledged();
		} catch (IOException e) {
			log.error("ElasticsearchClientFactory.createIndex", e);
		}

		return false;
	}

	/**
	 * 检查索引
	 *
	 * @param index
	 * @return
	 * @throws IOException
	 */
	public static boolean checkIndexExist(RestHighLevelClient client, String index) {
		try {
			GetIndexRequest request = new GetIndexRequest(index);
			return client.indices().exists(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			log.error("ElasticsearchClientFactory.checkIndexExist", e);
		}
		return false;
	}

	/**
	 * 删除索引
	 *
	 * @param client
	 * @param index
	 * @return
	 */
	public static boolean deleteIndex(RestHighLevelClient client, String index) {
		try {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
			AcknowledgedResponse response = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
			return response.isAcknowledged();
		} catch (IOException e) {
			log.error("ElasticsearchClientFactory.deleteIndex", e);
		}
		return false;
	}

}
