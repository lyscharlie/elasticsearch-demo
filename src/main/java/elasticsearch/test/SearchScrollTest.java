package elasticsearch.test;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSONObject;

import dataobject.CommonData;
import elasticsearch.common.BaseDocument;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

public class SearchScrollTest {

	public static void main(String[] args) {

		try {
			String index = "demo_test_scroll";
			String mappings = "{\"properties\":{\"code\":{\"type\":\"keyword\"},\"number\":{\"type\":\"integer\"}}}";

			// 连接elasticsearch
			RestHighLevelClient client = QueryTestUtils.initClient();

			// 创建index
			if (ElasticsearchUtils.checkIndexExist(client, index)) {
				ElasticsearchUtils.removeIndex(client, index);
			}
			ElasticsearchUtils.createIndex(client, index, mappings);

			QueryTestUtils.line("完成创建索引");

			List<BaseDocument> dataList = new ArrayList<>();
			for (int i = 1; i <= 100; i++) {
				CommonData data = new CommonData();
				data.setCode("code_" + i);
				data.setNumber(i);
				dataList.add(new BaseDocument(null, data));
			}

			// 批量添加数据
			BulkResponse bulkResponse = ElasticsearchUtils.saveBulkDocuments(client, index, dataList, true);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			QueryTestUtils.line("完成写入");

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(QueryBuilders.matchAllQuery());
			searchSourceBuilder.size(10);
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			Scroll scroll = new Scroll(TimeValue.timeValueSeconds(20));

			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.source(searchSourceBuilder);
			searchRequest.scroll(scroll);

			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			String scrollId = response.getScrollId();
			System.out.println("scrollId=[" + scrollId + "]");

			SearchHit[] searchHits = response.getHits().getHits();

			while (searchHits != null && searchHits.length > 0) {

				QueryTestUtils.line("分页");

				for (SearchHit item : searchHits) {
					System.out.println(item.getScore() + " ==> " + item.getSourceAsString());
				}

				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(scroll);
				response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
				searchHits = response.getHits().getHits();
			}

			QueryTestUtils.line();

			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			clearScrollRequest.addScrollId(scrollId);
			ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
			System.out.println(JSONObject.toJSONString(clearScrollResponse));
			QueryTestUtils.line();

			// 删除index
			ElasticsearchUtils.removeIndex(client, index);

			// 关闭
			client.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
