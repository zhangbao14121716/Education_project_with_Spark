package utils;

/**
 * @Author: Zhangbao
 * @Date: 15:29 2020/9/12
 * @Description:
 */

import java.io.InputStream;
import java.util.Properties;

/**
 * 读取文件配置工具类
 *
 *
 */
public class ConfigurationManager {
    public static Properties prop = new Properties();

    //将resource中的类信息加载到配置文件中--创建类执行
    static {
        try {
            //反射获取resource文件夹下面jdbc文件配置的流
            InputStream inputStream = ConfigurationManager.class.getClassLoader()
                    .getResourceAsStream("/comerce.properties");
            //配置加载流
            prop.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取配置项
    public static String getProperty(String key) {return prop.getProperty(key);}

    //获取布尔类型的配置项--把字符串转化为布尔类型
    public static Boolean getBooleanProperty(String key) {
        String value = prop.getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
