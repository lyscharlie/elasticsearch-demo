package elasticsearch.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.client.RestHighLevelClient;

import common.FileUtils;
import dataobject.CommonData;
import elasticsearch.common.ElasticsearchUtils;

public class QueryTestUtils {

	private static String scheme = "http";
	private static String host = "localhost";
	private static int port = 9200;

	/**
	 * 初始化客户端
	 *
	 * @return
	 */
	public static RestHighLevelClient initClient() {
		return ElasticsearchUtils.initClient(scheme, host, port);
	}

	/**
	 * 读取数据类型映射
	 *
	 * @return
	 */
	public static String mappings() {
		return FileUtils.readFile(QueryTestUtils.class.getResource("/dataobject/common_data_mapping.json").getPath(), "utf-8");
	}

	/**
	 * 中文数据
	 *
	 * @return
	 */
	public static List<CommonData> chineseList() {
		List<String> words = new ArrayList<>();
		words.add("韩都衣舍韩版2014秋冬新款女装蝙蝠袖连帽长袖连衣裙包邮");
		words.add("女装 长绒拉链连帽运动开衫 126418 优衣库");
		words.add("【懒猫洗衣】运动鞋/休闲鞋清洗保养3双 免费上门取送");
		words.add("[Midea/美的]美的蒸汽挂烫机正品 家用双杆挂式电熨斗熨");
		words.add("马克华菲羽绒服男士韩版羽绒外套 精选白鸭绒足量填充");
		words.add("马克华菲羽绒服女士韩版羽绒外套 精选白鸭绒足量填充包邮");
		words.add("马克华菲羽绒服老年韩版羽绒外套 精选白鸭绒足量填充");
		words.add("[Omega/欧米茄]蝶飞经典机械男表");
		words.add("七度空间优雅丝柔12包组合 定制专供 加量不加价");
		words.add("[温碧泉]透芯润五件套+送金稻定制蒸脸仪+旅行套+面膜");
		words.add("[SHUA/舒华]倒立机 腰椎颈椎牵引器 拉伸增高倒挂机");
		words.add("泸州老窖 60°泸州老窖泸州原浆珍品1500ml 三斤大坛酒");
		words.add("翠苑街道文三路477号华星科技大厦");

		List<CommonData> dataList = new ArrayList<>();
		for (int i = 0; i < words.size(); i++) {
			CommonData data = new CommonData();
			data.setName("test " + i);
			data.setDesc(words.get(i));
			data.setNumber(i);
			data.setTime(new Date());
			dataList.add(data);
		}

		return dataList;
	}

	/**
	 * 英文数据
	 *
	 * @return
	 */
	public static List<CommonData> englishList() {
		List<String> words = new ArrayList<>();
		words.add("Expert Tips When Migrating to Elastic Cloud Enterprise (ECE)");
		words.add("Building Effective Dashboards with Kibana and Elasticsearch");
		words.add("Intro to Canvas: A new way to tell visual stories in Kibana");
		words.add("Learn how to easily navigate a migration, and avoid common mistakes by adopting these simple, insightful tips.");
		words.add("Feel free to forward this invite to any colleagues");
		words.add("Learn to build visualizations quickly, easily, and effectively using Kibana and the Elastic Stack. ");
		words.add("Join this webinar to learn how you can start creating custom, infographic-style presentations with your live Elasticsearch data.");

		List<CommonData> dataList = new ArrayList<>();
		for (int i = 0; i < words.size(); i++) {
			CommonData data = new CommonData();
			data.setName("test " + i);
			data.setDesc(words.get(i));
			data.setNumber(i);
			data.setTime(new Date());
			dataList.add(data);
		}

		return dataList;
	}

	/**
	 * 自定义内容分割线
	 */
	public static void line() {
		System.out.println("-----------------------------------------------------------------------");
	}

	/**
	 * 自定义内容分割线
	 *
	 * @param words
	 */
	public static void line(String words) {
		System.out.println("------------------------------------------- " + words + " --------------------------------------------");
	}
}
