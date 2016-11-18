package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;


public class Config implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    public static final Properties CONFIG = new Properties();

    static{
        try {
            InputStream is = Config.class.getClassLoader().
                    getResourceAsStream("config.properties");
            CONFIG.load(is);
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
    }

    public static Properties getConfig(String config_file){
        Properties properties = new Properties();
        try {
            InputStream is = Config.class.getClassLoader().
                    getResourceAsStream(config_file);
            properties.load(is);
            logger.info("init load " + config_file);
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
        return properties;
    }
}
