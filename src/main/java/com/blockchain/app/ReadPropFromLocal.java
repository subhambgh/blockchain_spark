package com.blockchain.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadPropFromLocal {
    private static InputStream inputStream;
    String result = "";
    public static String getProperties(String key) throws IOException {
        Properties prop = new Properties();
        String propFileName = "config-Local.properties";
        inputStream = ReadPropFromLocal.class.getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
        return prop.getProperty(key);
    }
}
