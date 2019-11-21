package elasticsearch.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSONObject;

import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

public class SearchObjectArrayTest {

	public static void main(String[] args) {
		try {
			String keyword = "刘德华";

			List<String> nameList = Arrays.asList("张三", "李四", "王五", "赵六", "刘德华", "张学友", "黎明", "郭富城", "李连杰");

			List<Map<String, Object>> dataList = new ArrayList<>();
			for (int i = 1; i <= 20; i++) {
				Map<String, Object> map = new HashMap<>();
				map.put("class", i);
				List<Map<String, Object>> userList = new ArrayList<>();
				int num = RandomUtils.nextInt(1, 5);
				for (int j = 0; j < num; j++) {
					Map<String, Object> user = new HashMap<>();
					user.put("name", nameList.get(RandomUtils.nextInt(0, nameList.size() - 1)));
					user.put("age", RandomUtils.nextInt(1, 50));
					userList.add(user);
				}
				map.put("user", userList);
				System.out.println(JSONObject.toJSONString(map));
				dataList.add(map);
			}

			String index = "demo_array_test";
			String mappings = FileUtils.readFileToString(new File(QueryTestUtils.class.getResource("/mappings/student_class.json").getPath()), "utf-8");

			// 连接elasticsearch
			RestHighLevelClient client = QueryTestUtils.initClient();

			// 创建index
			if (ElasticsearchUtils.checkIndexExist(client, index)) {
				ElasticsearchUtils.removeIndex(client, index);
			}
			ElasticsearchUtils.createIndex(client, index, mappings);

			QueryTestUtils.line("完成创建索引");

			// 批量添加数据
			BulkResponse bulkResponse = ElasticsearchUtils.saveBulkDocumentsForObject(client, index, dataList, true);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			QueryTestUtils.line("完成写入");

			// 关键字

			TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("user.name", keyword);
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("user.age").lt(30);

			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
			boolQueryBuilder.must(termQueryBuilder);
			boolQueryBuilder.must(rangeQueryBuilder);

			NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery("user", boolQueryBuilder, ScoreMode.Avg);

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(nestedQueryBuilder);
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
