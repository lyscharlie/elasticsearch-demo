package elasticsearch.query.compound;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import dataobject.CommonData;
import elasticsearch.common.ElasticsearchUtils;
import elasticsearch.query.QueryTestUtils;

/**
 * 最佳字段查询
 */
public class DisjunctionMaxQueryTest {

	public static void main(String[] args) {
		try {

			String keyword = "马克华菲羽绒服女士韩版羽绒外套 精选白鸭绒足量填充";

			List<String> words1 = new ArrayList<String>();
			words1.add("韩都衣舍韩版连衣裙");
			words1.add("长绒拉链连帽运动开衫");
			words1.add("运动鞋/休闲鞋清洗保养");
			words1.add("美的蒸汽挂烫机");
			words1.add("马克华菲羽绒服男士韩版羽绒外套");
			words1.add("马克华菲羽绒服女士韩版羽绒外套");
			words1.add("马克华菲羽绒服老年韩版羽绒外套");
			words1.add("蝶飞经典机械男表");
			words1.add("七度空间优雅丝柔12包组合");
			words1.add("[温碧泉]透芯润五件套");
			words1.add("[SHUA/舒华]倒立机");
			words1.add("泸州老窖");
			words1.add("泸州老窖 华星科技大厦");

			List<String> words2 = new ArrayList<String>();
			words2.add("韩都衣舍韩版2014秋冬新款女装蝙蝠袖连帽长袖连衣裙");
			words2.add("女装 长绒拉链连帽运动开衫 126418 优衣库");
			words2.add("【懒猫洗衣】运动鞋/休闲鞋清洗保养3双 免费上门取送");
			words2.add("[Midea/美的]美的蒸汽挂烫机正品 家用双杆挂式电熨斗熨");
			words2.add("马克华菲羽绒服男士韩版羽绒外套 精选白鸭绒足量填充");
			words2.add("马克华菲羽绒服女士韩版羽绒外套 精选白鸭绒足量填充");
			words2.add("马克华菲羽绒服老年韩版羽绒外套 精选白鸭绒足量填充");
			words2.add("[Omega/欧米茄]蝶飞经典机械男表");
			words2.add("七度空间优雅丝柔12包组合 定制专供 加量不加价");
			words2.add("[温碧泉]透芯润五件套+送金稻定制蒸脸仪+旅行套+面膜");
			words2.add("[SHUA/舒华]倒立机 腰椎颈椎牵引器 拉伸增高倒挂机");
			words2.add("泸州老窖 60°泸州老窖泸州原浆珍品1500ml 三斤大坛酒");
			words2.add("翠苑街道文三路477号华星科技大厦");

			List<CommonData> dataList = new ArrayList<>();
			for (int i = 0; i < words1.size(); i++) {
				CommonData data = new CommonData();
				data.setName("test " + i);
				data.setDesc(words1.get(i));
				data.setMark(words2.get(i));
				data.setNumber(i);
				data.setTime(new Date());
				dataList.add(data);
			}

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

			DisMaxQueryBuilder disMaxQueryBuilder = QueryBuilders.disMaxQuery().add(QueryBuilders.matchQuery("desc", keyword)).add(QueryBuilders.matchQuery("mark", keyword));

			// 查询数据
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.query(disMaxQueryBuilder);
			System.out.println(searchSourceBuilder);

			QueryTestUtils.line();

			SearchResponse response = ElasticsearchUtils.getDocsByQuery(client, index, searchSourceBuilder);
			System.out.println(response.toString());

			QueryTestUtils.line();

			if (response.getHits().getTotalHits().value > 0) {
				for (SearchHit item : response.getHits().getHits()) {
					System.out.println(item.getSourceAsString());
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
