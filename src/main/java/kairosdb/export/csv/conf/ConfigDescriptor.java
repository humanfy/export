package kairosdb.export.csv.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);


  private Config config;

  private ConfigDescriptor() {
    config = new Config();
    loadProps();
  }

  public static ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  public Config getConfig() {
    return config;
  }

  private void loadProps() {
    String url = System.getProperty(Constants.REST_CONF, null);
    if (url != null) {
      InputStream inputStream;
      try {
        inputStream = new FileInputStream(new File(url));
      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find config file {}", url);
        return;
      }
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
        config.START_TIME = properties.getProperty("START_TIME", config.START_TIME);
        config.ENDED_TIME = properties.getProperty("ENDED_TIME", config.ENDED_TIME);
        config.DELETE_CSV = Boolean.parseBoolean(properties.getProperty("DELETE_CSV", "true"));
        config.INSERTINTOIOTDB = Boolean.parseBoolean(properties.getProperty("INSERTINTOIOTDB", "false"));
        config.THREAD_NUM = Integer.parseInt(properties.getProperty("THREAD_NUM", "128"));
        config.dirAbsolutePath = properties.getProperty("dirAbsolutePath", config.dirAbsolutePath);
      } catch (IOException e) {
        LOGGER.error("load properties error: ", e);
      }
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.error("Fail to close config file input stream", e);
      }
    } else {
      LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
    }
  }

  private static class ConfigDescriptorHolder {

    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }
}
