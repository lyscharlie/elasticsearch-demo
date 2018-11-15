package elasticsearch;

import org.elasticsearch.client.RestHighLevelClient;

public class Test {

	public static void main(String[] args) {

		try {
			String host = "localhost";
			int port = 9200;
			String index = "test1";

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

			// 新增数据
			// Map<String, Object> map = new HashMap<>();
			// map.put("name", "test1");
			// map.put("count", 50);
			// String id = ElasticsearchUtils.addData(client, index, "_doc", "1", map);
			// System.out.println(id);

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
