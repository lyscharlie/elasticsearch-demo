package elasticsearch;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

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
			String type = "_doc";

			String mappings = FileUtils.readFile("src/main/java/elasticsearch/test1_mapping.json", "utf-8");
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
				createIndexRequest.mapping(type, mappings, XContentType.JSON);
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
				Person p = new Person();
				p.setId(i + "");
				p.setName("test " + i);
				p.setAge(10 + i);
				p.setBirthday(DateUtils.addDays(new Date(), RandomUtils.nextInt(1, 1000) - 2000));
				if (i % 2 == 0) {
					p.setMark("这个是中国人");
				} else {
					p.setMark("这个是美国人");
				}
				personList.add(p);
			}

			BulkRequest bulkRequest = new BulkRequest();
			for (Person person : personList) {
				IndexRequest indexRequest = new IndexRequest();
				indexRequest.index(index);
				indexRequest.type(type);
				indexRequest.id(person.getId());
				indexRequest.source(JSONObject.toJSONString(person), XContentType.JSON);
				bulkRequest.add(indexRequest);
			}
			BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println(bulkItemResponse.getResponse().getId());
			}

			// 查询数据
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
			sourceBuilder.from(0);
			sourceBuilder.size(10);
			MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("mark", "中国");
			sourceBuilder.query(matchQueryBuilder);
			SearchRequest searchRequest = new SearchRequest(index);
			searchRequest.types(type);
			searchRequest.source(sourceBuilder);
			SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
			System.out.println(response);

			// 删除index
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
			DeleteIndexResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
			if (deleteIndexResponse.isAcknowledged()) {
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
