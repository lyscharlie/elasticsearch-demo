package elasticsearch.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import com.alibaba.fastjson.JSONObject;

import common.FileUtils;
import dataobject.CommonData;
import elasticsearch.common.ElasticsearchClientFactory;

/**
 * 短语搜索（不适用中文）
 * 
 * @author liyishi
 *
 */
public class MatchPhrasePrefixQueryBuilderTest {

	public static void main(String[] args) {
		String keyword = "Kibana Elasticsearch";

		List<String> words = new ArrayList<String>();
		words.add("Expert Tips When Migrating to Elastic Cloud Enterprise (ECE)");
		words.add("Building Effective Dashboards with Kibana and Elasticsearch");
		words.add("Intro to Canvas: A new way to tell visual stories in Kibana");
		words.add("Learn how to easily navigate a migration, and avoid common mistakes by adopting these simple, insightful tips.");
		words.add("Feel free to forward this invite to any colleagues");
		words.add("Learn to build visualizations quickly, easily, and effectively using Kibana and the Elastic Stack. ");
		words.add("Join this webinar to learn how you can start creating custom, infographic-style presentations with your live Elasticsearch data.");

		List<CommonData> dataList = new ArrayList<>();
		for (int i = 0; i < words.size(); i++) {
			CommonData data = new CommonData();
			data.setName("test " + i);
			data.setDesc(words.get(i));
			data.setNumber(i);
			data.setTime(new Date());
			dataList.add(data);
		}

		try {
			String scheme = "http";
			String host = "localhost";
			int port = 9200;

			String index = "demo_test";
			String type = "_doc";

			String mappings = FileUtils.readFile("src/main/java/dataobject/common_data_mapping.json", "utf-8");

			// 连接elasticsearch
			RestHighLevelClient client = ElasticsearchClientFactory.initClient(scheme, host, port);

			// 创建index
			if (!ElasticsearchClientFactory.checkIndexExist(client, index)) {
				ElasticsearchClientFactory.createIndex(client, index, type, mappings);
			}

			// 批量添加数据
			BulkRequest bulkRequest = new BulkRequest();
			for (CommonData data : dataList) {
				IndexRequest indexRequest = new IndexRequest();
				indexRequest.index(index);
				indexRequest.type(type);
				indexRequest.source(JSONObject.toJSONString(data), XContentType.JSON);
				bulkRequest.add(indexRequest);
			}
			bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);// 强制同步操作
			BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			// 查询数据
			MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = QueryBuilders.matchPhrasePrefixQuery("desc", keyword);

			// 高亮
			HighlightBuilder highlightBuilder = new HighlightBuilder();
			highlightBuilder.preTags("<strong>");// 设置前缀
			highlightBuilder.postTags("</strong>");// 设置后缀
			highlightBuilder.field("desc");// 设置高亮字段

			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.from(0);
			searchSourceBuilder.size(10);
			searchSourceBuilder.query(matchPhrasePrefixQueryBuilder);
			searchSourceBuilder.highlighter(highlightBuilder);
			System.out.println(searchSourceBuilder);

			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.types(type);
			searchRequest.source(searchSourceBuilder);

			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			System.out.println(response.toString());
			if (response.getHits().totalHits > 0) {
				for (SearchHit item : response.getHits().getHits()) {
					System.out.println(item.getScore() + "==>" + item.getHighlightFields());
					System.out.println(item.getSourceAsString());
				}
			}

			// 删除index
			ElasticsearchClientFactory.deleteIndex(client, index);

			// 关闭
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
