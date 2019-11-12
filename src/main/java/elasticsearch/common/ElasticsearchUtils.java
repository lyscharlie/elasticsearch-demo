package elasticsearch.common;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
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
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
	 * 设置索引对象 mapping
	 *
	 * @param client
	 * @param index
	 * @param mappings
	 * @return
	 */
	public static boolean putIndexMapping(RestHighLevelClient client, String index, String mappings) {
		try {
			PutMappingRequest putMappingRequest = new PutMappingRequest(index);
			putMappingRequest.source(mappings, XContentType.JSON);
			AcknowledgedResponse acknowledgedResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
			return acknowledgedResponse.isAcknowledged();
		} catch (IOException e) {
			log.error("ElasticsearchUtils.putIndexMapping", e);
		}
		return false;
	}

	/**
	 * 修改配置
	 *
	 * @param client
	 * @param index
	 * @param settings
	 * @return
	 */
	public static boolean updateIndexSettings(RestHighLevelClient client, String index, Settings settings) {
		try {
			UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index);
			updateSettingsRequest.settings(settings);
			AcknowledgedResponse acknowledgedResponse = client.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
			return acknowledgedResponse.isAcknowledged();
		} catch (IOException e) {
			log.error("ElasticsearchUtils.updateIndexSettings", e);
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
	 * @param client
	 * @param index
	 * @param document
	 * @param sync     是否同步
	 * @return
	 */
	public static IndexResponse saveDocument(RestHighLevelClient client, String index, BaseDocument document, boolean sync) {
		try {
			IndexRequest indexRequest = new IndexRequest();
			indexRequest.index(index);
			indexRequest.id(document.getId());
			indexRequest.source(JSONObject.toJSONString(document.getObject()), XContentType.JSON);

			if (sync) {
				indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			}
			IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
			return indexResponse;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.saveDocument", e);
		}
		return null;
	}

	/**
	 * 批量写入
	 *
	 * @param <T>
	 * @param client
	 * @param index
	 * @param documentList
	 * @param sync         是否同步
	 * @return
	 */
	public static <T> BulkResponse saveBulkDocuments(RestHighLevelClient client, String index, List<BaseDocument> documentList, boolean sync) {
		try {
			BulkRequest bulkRequest = new BulkRequest();

			for (BaseDocument document : documentList) {
				IndexRequest indexRequest = new IndexRequest();
				indexRequest.index(index);
				if (StringUtils.isNotBlank(document.getId())) {
					indexRequest.id(document.getId());
				}
				indexRequest.source(JSONObject.toJSONString(document.getObject()), XContentType.JSON);
				bulkRequest.add(indexRequest);
			}
			if (sync) {
				bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			}
			BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);

			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.saveBulkDocuments", e);
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
	public static GetResponse searchDocumentById(RestHighLevelClient client, String index, String id) {
		try {
			GetRequest request = new GetRequest();
			request.index(index);
			request.id(id);
			GetResponse response = client.get(request, RequestOptions.DEFAULT);
			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.searchDocumentById", e);
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
	public static SearchResponse searchDocumentsByQuery(RestHighLevelClient client, String index, SearchSourceBuilder sourceBuilder) {
		try {
			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.source(sourceBuilder);
			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.searchDocumentsByQuery", e);
		}
		return null;
	}

	/**
	 * 查询数量
	 *
	 * @param client
	 * @param index
	 * @param queryBuilder
	 * @return
	 */
	public static CountResponse countDocumentsByQuery(RestHighLevelClient client, String index, QueryBuilder queryBuilder) {
		try {
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.query(null != queryBuilder ? queryBuilder : QueryBuilders.matchAllQuery());

			CountRequest countRequest = new CountRequest(index);
			countRequest.source(sourceBuilder);

			CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
			return countResponse;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.countDocumentsByQuery", e);
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
	public static DeleteResponse removeDocumentById(RestHighLevelClient client, String index, String id) {
		try {
			DeleteRequest deleteRequest = new DeleteRequest(index);
			deleteRequest.id(id);
			DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
			return response;
		} catch (Exception e) {
			log.error("ElasticsearchUtils.removeDocumentById", e);
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
	public static BulkByScrollResponse removeDocumentsByQuery(RestHighLevelClient client, String index, SearchSourceBuilder sourceBuilder) {
		try {
			DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
			deleteByQueryRequest.setQuery(sourceBuilder.query());
			deleteByQueryRequest.setRefresh(true);
			BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
			return bulkByScrollResponse;
		} catch (IOException e) {
			log.error("ElasticsearchUtils.removeDocumentsByQuery", e);
		}
		return null;
	}
}