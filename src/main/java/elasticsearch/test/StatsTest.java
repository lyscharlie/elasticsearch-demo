package elasticsearch.test;

import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSONObject;

import dataobject.CommonData;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

public class StatsTest {

	public static void main(String[] args) {
		try {

			List<CommonData> dataList = QueryTestUtils.chineseList();

			String index = "demo_test";
			String mappings = QueryTestUtils.mappings();

			// 连接elasticsearch
			RestHighLevelClient client = QueryTestUtils.initClient();

			// 创建index
			if (ElasticsearchUtils.checkIndexExist(client, index)) {
				ElasticsearchUtils.removeIndex(client, index);
			}
			ElasticsearchUtils.createIndex(client, index, mappings);

			QueryTestUtils.line("完成创建索引");

			// 批量添加数据
			BulkResponse bulkResponse = ElasticsearchUtils.saveBulkDocuments(client, index, dataList, true);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			QueryTestUtils.line("完成写入");

			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("number").gte(5);
			StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders.stats("stats_by_number").field("number");

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.size(10);
			searchSourceBuilder.query(rangeQueryBuilder);
			searchSourceBuilder.aggregation(statsAggregationBuilder);
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			SearchResponse response = ElasticsearchUtils.searchDocumentsByQuery(client, index, searchSourceBuilder);
			System.out.println(response.toString());

			QueryTestUtils.line();

			System.out.println(JSONObject.toJSONString(response.getAggregations().getAsMap().get("stats_by_number")));

			// ParsedTerms groupByCat = (ParsedTerms) response.getAggregations().asMap().get("group_by_cat");
			//
			// QueryTestUtils.line();
			//
			// for (Terms.Bucket bucket : groupByCat.getBuckets()) {
			// 	ParsedSum sum = (ParsedSum) bucket.getAggregations().asMap().get("sum_number");
			// 	System.out.println(bucket.getKeyAsString() + "===>" + sum.getValueAsString());
			// }

			QueryTestUtils.line();

			if (response.getHits().getTotalHits().value > 0) {
				for (SearchHit item : response.getHits().getHits()) {
					System.out.println(item.getScore() + " ==> " + item.getSourceAsString());
				}
			}

			// 删除index
			ElasticsearchUtils.removeIndex(client, index);

			// 关闭
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
