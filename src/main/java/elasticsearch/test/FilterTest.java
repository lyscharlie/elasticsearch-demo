package elasticsearch.test;

import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import elasticsearch.common.BaseDocument;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

/**
 * Filter
 *
 * @author liyishi
 */
public class FilterTest {

	public static void main(String[] args) {

		try {
			List<BaseDocument> dataList = QueryTestUtils.chineseList();

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

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.from(0);
			searchSourceBuilder.size(10);
			searchSourceBuilder.postFilter(QueryBuilders.termQuery("cat", "2"));
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			SearchResponse response = ElasticsearchUtils.searchDocumentsByQuery(client, index, searchSourceBuilder);
			System.out.println(response.toString());

			QueryTestUtils.line();

			if (response.getHits().getTotalHits().value > 0) {
				for (SearchHit item : response.getHits().getHits()) {
					System.out.println(item.getScore() + "==>" + item.getHighlightFields() + " ==> " + item.getSourceAsString());
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
