package elasticsearch.common;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchUtils {

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
			log.error("ElasticsearchUtils.createIndex", e);
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
			log.error("ElasticsearchUtils.checkIndexExist", e);
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
	public static boolean removeIndex(RestHighLevelClient client, String index) {
		try {
			DeleteIndexRequest request = new DeleteIndexRequest(index);
			AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
			return response.isAcknowledged();
		} catch (IOException e) {
			log.error("ElasticsearchUtils.deleteIndex", e);
		}
		return false;
	}

	/**
	 * 写入数据
	 *
	 * @param index
	 * @param object
	 * @param sync   是否同步
	 * @return
	 */
	public static IndexResponse saveDoc(RestHighLevelClient client, String index, String id, Object object, boolean sync) {
		try {
			IndexRequest indexRequest = new IndexRequest();
			indexRequest.index(index);
			indexRequest.id(id);
			indexRequest.source(JSONObject.toJSONString(object), XContentType.JSON);

			if (sync) {
				indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			}
			IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
			return indexResponse;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.saveDoc", e);
		}
		return null;
	}

	/**
	 * 批量写入
	 *
	 * @param <T>
	 * @param client
	 * @param index
	 * @param objectList
	 * @param sync       是否同步
	 * @return
	 */
	public static <T> BulkResponse saveBulkDocs(RestHighLevelClient client, String index, List<T> objectList, boolean sync) {
		try {
			BulkRequest bulkRequest = new BulkRequest();
			// bulkRequest.timeout("10000");
			for (Object object : objectList) {
				IndexRequest indexRequest = new IndexRequest();
				indexRequest.index(index);
				indexRequest.source(JSONObject.toJSONString(object), XContentType.JSON);
				bulkRequest.add(indexRequest);
			}
			if (sync) {
				bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			}
			BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

			// for (BulkItemResponse bulkItemResponse : response.getItems()) {
			// 	System.out.println(bulkItemResponse.getResponse().getId());
			// }
			// System.out.println(JSONObject.toJSON(response));

			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.saveBulkDocs", e);
		}
		return null;
	}

	/**
	 * 根据主键查询数据
	 *
	 * @param client
	 * @param index
	 * @param id
	 * @return
	 */
	public static GetResponse getDocById(RestHighLevelClient client, String index, String id) {
		try {
			GetRequest request = new GetRequest();
			request.index(index);
			request.id(id);
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.getDocById", e);
		}
		return null;
	}

	/**
	 * 查询
	 *
	 * @param client
	 * @param index
	 * @param sourceBuilder
	 * @return
	 */
	public static SearchResponse getDocsByQuery(RestHighLevelClient client, String index, SearchSourceBuilder sourceBuilder) {
		try {
			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.source(sourceBuilder);
			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.getDocsByQuery", e);
		}
		return null;
	}

	/**
	 * 根据主键删除数据
	 *
	 * @param client
	 * @param index
	 * @param id
	 * @return
	 */
	public static DeleteResponse removeDocById(RestHighLevelClient client, String index, String id) {
		try {
			DeleteRequest deleteRequest = new DeleteRequest(index);
			deleteRequest.id(id);
			DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.removeDocById", e);
		}
		return null;
	}

	/**
	 * 根据条件删除数据
	 *
	 * @param client
	 * @param index
	 * @param sourceBuilder
	 */
	public static BulkByScrollResponse removeDocsByQuery(RestHighLevelClient client, String index, SearchSourceBuilder sourceBuilder) {
		try {
			DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
			deleteByQueryRequest.setQuery(sourceBuilder.query());
			deleteByQueryRequest.setRefresh(true);
			BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
			return bulkByScrollResponse;
		} catch (IOException e) {
			log.error("ElasticsearchUtils.removeDocsByQuery", e);
		}
		return null;
	}
}