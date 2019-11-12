package elasticsearch.test;

import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import elasticsearch.common.BaseDocument;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

public class NumberTest {

	public static void main(String[] args) {
		try {

			List<BaseDocument> dataList = QueryTestUtils.englishList();

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

			// RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("number").gte(1).lt(5);
			// RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("number").from(1, true).to(5, false);
			// TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("number", Arrays.asList(1, 2, 3, 4, 5));
			TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("number", "5");

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(termsQueryBuilder);
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			SearchResponse response = ElasticsearchUtils.searchDocumentsByQuery(client, index, searchSourceBuilder);
			System.out.println(response.toString());

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
