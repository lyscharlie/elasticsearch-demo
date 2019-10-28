package elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import elasticsearch.common.BaseDocument;
import elasticsearch.common.ElasticsearchUtils;

public class Test {

	public static void main(String[] args) {

		try {
			String host = "localhost";
			int port = 9200;
			String index = "test1";

			RestHighLevelClient client = ElasticsearchUtils.initClient("http", host, port);

			// 创建
			if (!ElasticsearchUtils.checkIndexExist(client, index)) {
				boolean success = ElasticsearchUtils.createIndex(client, index, null);
				if (success) {
					System.out.println("创建索引成功");
				} else {
					System.out.println("创建索引失败");
				}
			} else {
				System.out.println("索引已存在");
			}

			// boolean success = ElasticsearchUtils.createIndex(client, index);

			// 单个新增数据
			Map<String, Object> map = new HashMap<>();
			map.put("name", "test 1");
			map.put("count", RandomUtils.nextInt(0, 100));
			IndexResponse indexResponse = ElasticsearchUtils.saveDoc(client, index, new BaseDocument("1", map), true);
			String id = indexResponse.getId();
			System.out.println(id);

			// 单个查询
			String result1 = ElasticsearchUtils.getDocById(client, index, "1").toString();
			System.out.println(result1);

			// 批量新增数据
			List<BaseDocument> dataList = new ArrayList<>();
			for (int i = 1; i <= 10; i++) {
				Map<String, Object> dataMap = new HashMap<>();
				map.put("id", i + "");
				dataMap.put("name", "test " + i);
				dataMap.put("count", RandomUtils.nextInt(0, 100));
				dataList.add(new BaseDocument(null, map));
			}
			String result3 = ElasticsearchUtils.saveBulkDocs(client, index, dataList, true).toString();
			System.out.println(result3);

			// 查询
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.from(0);
			sourceBuilder.size(10);
			MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "test");
			sourceBuilder.query(matchQueryBuilder);
			String result2 = ElasticsearchUtils.getDocsByQuery(client, index, sourceBuilder).toString();
			System.out.println(sourceBuilder);
			System.out.println(result2);

			// 删除
			if (ElasticsearchUtils.removeIndex(client, index)) {
				System.out.println("删除索引成功");
			} else {
				System.out.println("删除索引失败");
			}

			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
