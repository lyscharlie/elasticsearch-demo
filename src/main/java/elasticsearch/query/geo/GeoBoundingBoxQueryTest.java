package elasticsearch.query.geo;

import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import dataobject.CommonData;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

public class GeoBoundingBoxQueryTest {

	public static void main(String[] args) {
		try {
			String keyword = "easily Elasticsearch";

			List<CommonData> dataList = QueryTestUtils.englishList();

			String index = "demo_test";
			String mappings = QueryTestUtils.mappings();

			// 连接elasticsearch
			RestHighLevelClient client = QueryTestUtils.initClient();

			// 创建index
			if (!ElasticsearchUtils.checkIndexExist(client, index)) {
				ElasticsearchUtils.createIndex(client, index, mappings);
			}

			QueryTestUtils.line("完成创建索引");

			// 批量添加数据
			BulkResponse bulkResponse = ElasticsearchUtils.saveBulkDocs(client, index, dataList, true);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			QueryTestUtils.line("完成写入");

			// 查询数据
			GeoBoundingBoxQueryBuilder geoBoundingBoxQueryBuilder = QueryBuilders.geoBoundingBoxQuery("location").setCorners(new GeoPoint(20d, 100), new GeoPoint(30d, 180));

			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(geoBoundingBoxQueryBuilder);
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
