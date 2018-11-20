package elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import elasticsearch.common.ElasticsearchUtils;

public class Test {

	public static void main(String[] args) {

		try {
			String host = "localhost";
			int port = 9200;
			String index = "test1";
			String type = "_doc";

			RestHighLevelClient client = ElasticsearchUtils.initClient(host, port);

			// 创建
			if (!ElasticsearchUtils.checkIndexExist(client, index)) {
				boolean success = ElasticsearchUtils.createIndex(client, index);
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
			String id = ElasticsearchUtils.addData(client, index, type, "1", map, true);
			System.out.println(id);

			// 单个查询
			String result1 = ElasticsearchUtils.getDataById(client, index, type, "1");
			System.out.println(result1);

			// 批量新增数据
			List<Map<String, Object>> mapList = new ArrayList<>();
			for (int i = 1; i <= 10; i++) {
				Map<String, Object> dataMap = new HashMap<>();
				map.put("id", i + "");
				dataMap.put("name", "test " + i);
				dataMap.put("count", RandomUtils.nextInt(0, 100));
				mapList.add(dataMap);
			}
			String result3 = ElasticsearchUtils.addBulkDatas(client, index, type, mapList, true);
			System.out.println(result3);

			// 查询
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.from(0);
			sourceBuilder.size(10);
			MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "test");
			sourceBuilder.query(matchQueryBuilder);
			String result2 = ElasticsearchUtils.query(client, index, type, sourceBuilder);
			System.out.println(sourceBuilder);
			System.out.println(result2);

			// 删除
			if (ElasticsearchUtils.deleteIndex(client, index)) {
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
