package hr.kkinfo;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

public class DatabaseConfig {

    private String url;
    private String user;
    private String password;

    public DatabaseConfig() {
        loadProperties();
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("database.properties")) {
            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find database.properties");
                return;
            }

            // Load properties file
            prop.load(input);

            // Assign properties to variables
            url = prop.getProperty("db.url");
            user = prop.getProperty("db.user");
            password = prop.getProperty("db.password");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    // You can add other methods here as needed
}
