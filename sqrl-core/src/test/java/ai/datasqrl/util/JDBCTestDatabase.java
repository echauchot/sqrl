package ai.datasqrl.util;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.DiscoveryConfiguration.MetaData;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration.Dialect;
import com.google.common.base.Preconditions;
import java.util.Properties;
import lombok.Getter;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class JDBCTestDatabase implements DatabaseHandle {

  private final Properties properties;
  @Getter
  private final PostgreSQLContainer postgreSQLContainer;

  public JDBCTestDatabase(IntegrationTestSettings.DatabaseEngine dbType) {
    if (dbType == IntegrationTestSettings.DatabaseEngine.LOCAL) {
      Preconditions.checkArgument(hasLocalDbConfig());
      this.properties = PropertiesUtil.properties;
      this.postgreSQLContainer = null;
    } else if (dbType == IntegrationTestSettings.DatabaseEngine.POSTGRES){
      DockerImageName image = DockerImageName.parse("postgres:14.2");
      postgreSQLContainer = new PostgreSQLContainer(image)
          .withDatabaseName(MetaData.DEFAULT_DATABASE);
      postgreSQLContainer.start();

      this.properties = new Properties();
      this.properties.put("db.host", postgreSQLContainer.getHost());
      this.properties.put("db.port", postgreSQLContainer.getMappedPort(5432));
      this.properties.put("db.dialect", Dialect.POSTGRES.toString());
      this.properties.put("db.driverClassName", "org.postgresql.Driver");
      this.properties.put("db.username", "test");
      this.properties.put("db.password", "test");
      this.properties.put("db.url", postgreSQLContainer.getJdbcUrl().substring(0,
          postgreSQLContainer.getJdbcUrl().lastIndexOf("/")));
    } else throw new UnsupportedOperationException("Not a supported db type: " + dbType);
  }

  private static boolean hasLocalDbConfig() {
    return !PropertiesUtil.properties.isEmpty();
  }

  public JDBCConfiguration getJdbcConfiguration() {
    return JDBCConfiguration.builder()
        .host((String)properties.get("db.host"))
        .port((Integer)properties.get("db.port"))
        .dbURL((String)properties.get("db.url"))
        .driverName((String)properties.get("db.driverClassName"))
        .dialect(Dialect.valueOf((String)properties.get("db.dialect")))
        .user((String)properties.get("db.username"))
        .password((String)properties.get("db.password"))
        .build();
  }

  @Override
  public void cleanUp() {
    if (postgreSQLContainer!=null) {
      postgreSQLContainer.stop();
    }
  }
}
