package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读写文件工具类
 * 
 * @author LiYishi
 */
public class FileUtils {

	private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

	/**
	 * 读取文件
	 * 
	 * @param path
	 * @param charset
	 * @return
	 */
	public static String readFile(String path, String charset) {
		StringBuffer sb = new StringBuffer();

		try {
			File file = new File(path);

			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), charset);
				BufferedReader bufferedReader = new BufferedReader(read);
				String lineTxt = null;
				while ((lineTxt = bufferedReader.readLine()) != null) {
					sb.append(StringUtils.trim(lineTxt));
				}
				read.close();

				String text = sb.toString();
				text = StringUtils.replace(text, " ", "");
			} else {
				logger.error("找不到指定的文件：" + path);
			}

		} catch (Exception e) {
			logger.error("读取文件内容出错", e);
		}

		return sb.toString();
	}

	/**
	 * 写文件
	 * 
	 * @param path
	 * @param text
	 * @param charset
	 * @return
	 */
	public static boolean writeFile(String path, String text, String charset) {
		try {
			File file = new File(path);

			if (!file.exists()) {
				file.createNewFile();
			}

			FileOutputStream out = new FileOutputStream(file, false);
			out.write(text.getBytes(charset));// 注意需要转换对应的字符集
			out.close();

			logger.info("写文件完成：" + path);

			return true;
		} catch (Exception e) {
			logger.error("写入文件内容出错", e);
			return false;
		}
	}
}
