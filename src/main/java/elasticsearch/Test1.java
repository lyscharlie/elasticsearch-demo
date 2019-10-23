package elasticsearch;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import com.alibaba.fastjson.JSONObject;

import common.FileUtils;
import dataobject.Person;

public class Test1 {

	public static void main(String[] args) {

		try {
			String scheme = "http";
			String host = "localhost";
			int port = 9200;

			String index = "test2";

			String mappings = FileUtils.readFile("src/main/java/dataobject/person_mapping.json", "utf-8");
			System.out.println(mappings);

			// 连接elasticsearch
			RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));

			// 创建index
			GetIndexRequest request = new GetIndexRequest();
			request.indices(index);
			if (client.indices().exists(request, RequestOptions.DEFAULT)) {
				System.out.println("索引" + index + "已创建");
			} else {
				CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
				createIndexRequest.mapping(mappings, XContentType.JSON);
				CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
				if (createIndexResponse.isAcknowledged()) {
					System.out.println("创建索引成功");
				} else {
					System.out.println("创建索引失败");
					client.close();
					return;
				}
			}

			// 新增数据
			List<Person> personList = new ArrayList<>();
			for (int i = 1; i <= 10; i++) {
				Person person = new Person();
				person.setId(i + "");
				person.setName("test " + i);
				person.setSex(i % 2 + 1);
				person.setAge(10 + i);
				person.setBirthday(DateUtils.addDays(new Date(), RandomUtils.nextInt(1, 1000) - 2000));
				if (i % 2 == 0) {
					person.setMark("这个是中国人");
				} else {
					person.setMark("这个是美国人");
				}
				personList.add(person);
			}

			BulkRequest bulkRequest = new BulkRequest();
			for (Person person : personList) {
				IndexRequest indexRequest = new IndexRequest();
				indexRequest.index(index);
				indexRequest.id(person.getId());
				indexRequest.source(JSONObject.toJSONString(person), XContentType.JSON);
				// indexRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);// 强制同步操作
				bulkRequest.add(indexRequest);
			}
			bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE);// 强制同步操作
			BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			// Thread.sleep(5000L);

			// 关键词查询数据
			MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("mark", "中国");

			// 高亮
			HighlightBuilder highlightBuilder = new HighlightBuilder();
			highlightBuilder.preTags("<strong>");// 设置前缀
			highlightBuilder.postTags("</strong>");// 设置后缀
			highlightBuilder.field("mark");// 设置高亮字段

			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.from(0);
			searchSourceBuilder.size(10);
			searchSourceBuilder.fetchSource(new String[]{"name", "mark"}, null);
			searchSourceBuilder.query(matchQueryBuilder);
			searchSourceBuilder.highlighter(highlightBuilder);

			System.out.println(searchSourceBuilder);

			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.source(searchSourceBuilder);
			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			System.out.println(response.toString());
			if (response.getHits().getTotalHits().value > 0) {
				for (SearchHit item : response.getHits().getHits()) {
					Map<String, Object> map = item.getSourceAsMap();
					if (null != item.getHighlightFields()) {
						for (String field : item.getHighlightFields().keySet()) {
							Text[] texts = item.getHighlightFields().get(field).getFragments();
							map.put(field, texts[0]);
						}
						System.out.println(map);
					}
					// System.out.println(item.getHighlightFields());
					// System.out.println(item.getSourceAsString());
				}
			}

			// 删除index
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
			AcknowledgedResponse acknowledgedResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
			if (acknowledgedResponse.isAcknowledged()) {
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
