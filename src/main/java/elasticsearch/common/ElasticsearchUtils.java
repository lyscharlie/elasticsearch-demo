package elasticsearch.common;

import java.io.IOException;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class ElasticsearchUtils {

	private static Logger logger = LoggerFactory.getLogger(ElasticsearchUtils.class);

	/**
	 * 初始化客户端
	 * 
	 * @param host
	 * @param port
	 * @return
	 */
	public static RestHighLevelClient initClient(String host, int port) {
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));
		return client;
	}

	/**
	 * 创建索引
	 *
	 * @param index（index名必须全小写，否则报错）
	 * @return
	 */
	public static boolean createIndex(RestHighLevelClient client, String index) {
		try {
			CreateIndexRequest request = new CreateIndexRequest(index);
			CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			if (indexResponse.isAcknowledged()) {
				logger.info("创建索引成功");
			} else {
				logger.info("创建索引失败");
			}
			return indexResponse.isAcknowledged();
		} catch (IOException e) {
			logger.error("ElasticsearchUtils.createIndex", e);
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
			GetIndexRequest request = new GetIndexRequest();
			request.indices(index);
			return client.indices().exists(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("ElasticsearchUtils.checkIndexExist", e);
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
			DeleteIndexRequest request = new DeleteIndexRequest(index);
			DeleteIndexResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
			return response.isAcknowledged();
		} catch (IOException e) {
			logger.error("ElasticsearchUtils.deleteIndex", e);
		}
		return false;
	}

	/**
	 * 插入数据
	 * 
	 * @param index
	 * @param type
	 * @param object
	 * @param sync
	 *            是否同步
	 * @return
	 */
	public static String addData(RestHighLevelClient client, String index, String type, String id, Object object, boolean sync) {
		try {
			IndexRequest indexRequest = new IndexRequest(index, type, id);
			indexRequest.index(index);
			indexRequest.type(type);
			indexRequest.source(JSONObject.toJSONString(object), XContentType.JSON);
			if (sync) {
				indexRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
			}
			IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
			return indexResponse.getId();
		} catch (Exception e) {
			logger.error("ElasticsearchUtils.addData", e);
		}
		return null;
	}

	/**
	 * 批量添加
	 * 
	 * @param <T>
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param objectList
	 * @param sync
	 *            是否同步
	 * @return
	 */
	public static <T> String addBulkDatas(RestHighLevelClient client, String index, String type, List<T> objectList, boolean sync) {
		try {
			BulkRequest bulkRequest = new BulkRequest();
			for (Object object : objectList) {
				IndexRequest indexRequest = new IndexRequest();
				indexRequest.index(index);
				indexRequest.type(type);
				indexRequest.source(JSONObject.toJSONString(object), XContentType.JSON);
				bulkRequest.add(indexRequest);
			}
			if (sync) {
				bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
			}
			BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

			for (BulkItemResponse bulkItemResponse : response.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}
			System.out.println(JSONObject.toJSON(response));

			return response.toString();
		} catch (Exception e) {
			logger.error("ElasticsearchUtils.addBulkDatas", e);
		}
		return null;
	}

	/**
	 * 根据主键查询数据
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public static String getDataById(RestHighLevelClient client, String index, String type, String id) {
		try {
			GetRequest request = new GetRequest();
			request.index(index);
			request.type(type);
			request.id(id);
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			return response.toString();
		} catch (Exception e) {
			logger.error("ElasticsearchUtils.getDataById", e);
		}
		return null;
	}

	/**
	 * 查询
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param sourceBuilder
	 * @return
	 */
	public static String query(RestHighLevelClient client, String index, String type, SearchSourceBuilder sourceBuilder) {
		try {
			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.types(type);
			searchRequest.source(sourceBuilder);
			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			return response.toString();
		} catch (Exception e) {
			logger.error("ElasticsearchUtils.query", e);
		}
		return null;
	}

	public static void deleteById() {

	}

	public static void deleteByQuery() {

	}
}
