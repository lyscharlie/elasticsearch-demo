package elasticsearch.test;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSONObject;

import dataobject.CommonData;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

public class DateTest {

	public static void main(String[] args) {

		try {

			List<CommonData> dataList = QueryTestUtils.chineseList();

			// 日期做随机处理
			Date now = new Date();
			for (CommonData data : dataList) {
				boolean b = RandomUtils.nextBoolean();
				int num = b ? 1 : -1;
				data.setTime(DateUtils.addDays(now, num * RandomUtils.nextInt(1, 10)));
			}

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
			RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("time").lte("now");
			searchSourceBuilder.from(0);
			searchSourceBuilder.size(10);
			searchSourceBuilder.query(rangeQueryBuilder);
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			SearchResponse response = ElasticsearchUtils.searchDocumentsByQuery(client, index, searchSourceBuilder);
			System.out.println(response.toString());

			QueryTestUtils.line();

			System.out.println(response.getHits().getTotalHits().value);

			QueryTestUtils.line();

			if (response.getHits().getTotalHits().value > 0) {
				for (SearchHit item : response.getHits().getHits()) {
					CommonData data = JSONObject.parseObject(item.getSourceAsString(), CommonData.class);
					System.out.println(DateFormatUtils.format(data.getTime(),"yyyy-MM-dd HH:mm:ss"));
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
