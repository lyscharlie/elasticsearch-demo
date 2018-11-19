package elasticsearch.common;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchClientFactory {

	private static Logger logger = LoggerFactory.getLogger(ElasticsearchClientFactory.class);

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
	 * @param type
	 * @param mappings
	 * @return
	 */
	public static boolean createIndex(RestHighLevelClient client, String index, String type, String mappings) {
		try {
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
			if (StringUtils.isNoneBlank(type, mappings)) {
				createIndexRequest.mapping(type, mappings, XContentType.JSON);
			}
			CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
			if (createIndexResponse.isAcknowledged()) {
				logger.info("创建索引成功");
			} else {
				logger.info("创建索引失败");
			}
			return createIndexResponse.isAcknowledged();
		} catch (IOException e) {
			logger.error("ElasticsearchClientFactory.createIndex", e);
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
			GetIndexRequest GetIndexRequest = new GetIndexRequest();
			GetIndexRequest.indices(index);
			return client.indices().exists(GetIndexRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("ElasticsearchClientFactory.checkIndexExist", e);
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
			DeleteIndexResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
			return deleteIndexResponse.isAcknowledged();
		} catch (IOException e) {
			logger.error("ElasticsearchClientFactory.deleteIndex", e);
		}
		return false;
	}

}
