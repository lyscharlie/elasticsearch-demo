package elasticsearch.query.compound;

import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import dataobject.CommonData;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

/**
 * 组合多个叶子或复合查询子句的默认查询
 */
public class BoolQueryTest {

	public static void main(String[] args) {

		try {

			String keyword1 = "韩都衣舍";
			String keyword2 = "马克华菲";
			String keyword3 = "包邮";
			String keyword4 = "男士";
			String keyword5 = "老年";

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
			BulkResponse bulkResponse = ElasticsearchUtils.saveBulkDocs(client, index, dataList, true);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			QueryTestUtils.line("完成写入");

			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
			boolQueryBuilder.should(QueryBuilders.matchQuery("desc", keyword1));
			boolQueryBuilder.should(QueryBuilders.matchQuery("desc", keyword2));
			boolQueryBuilder.must(QueryBuilders.matchQuery("desc", keyword3));
			boolQueryBuilder.mustNot(QueryBuilders.matchQuery("desc", keyword4));
			boolQueryBuilder.filter(QueryBuilders.matchQuery("desc", keyword5));

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(boolQueryBuilder);
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			SearchResponse response = ElasticsearchUtils.getDocsByQuery(client, index, searchSourceBuilder);
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
