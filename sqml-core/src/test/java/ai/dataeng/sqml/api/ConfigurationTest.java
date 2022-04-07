package ai.dataeng.sqml.api;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.config.EnvironmentConfiguration;
import ai.dataeng.sqml.config.GlobalConfiguration;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.io.formats.JsonLineFormat;
import ai.dataeng.sqml.io.impl.file.DirectorySinkImplementation;
import ai.dataeng.sqml.io.impl.file.DirectorySourceImplementation;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

public class ConfigurationTest {

    public static Path resourceDir = Paths.get("src","test","resources");
    public static Path configYml = resourceDir.resolve("simple-config.yml");

    public static final Path dbPath = Path.of("tmp");
    public static final String jdbcURL = "jdbc:h2:"+dbPath.toAbsolutePath();

    public static final Path DATA_DIR = resourceDir.resolve("data");

    @BeforeEach
    public void deleteDatabase() throws IOException {
        FileUtils.cleanDirectory(ConfigurationTest.dbPath.toFile());
    }

    @Test
    public void testConfigFromFile() {
        GlobalConfiguration config = GlobalConfiguration.fromFile(configYml);
        validateConfig(config);
        assertEquals(config.getEngines().getJdbc().getDbURL(),"jdbc:h2:tmp/output");
        assertNotNull(config.getEngines().getFlink());
        assertEquals(config.getEnvironment().getMetastore().getDatabase(),"system");
        assertEquals(1, config.getSources().size());
        assertTrue(config.getSources().get(0).getSource() instanceof DirectorySourceImplementation);
        assertEquals(1, config.getSinks().size());
        assertTrue(config.getSinks().get(0).getSink() instanceof DirectorySinkImplementation);
        assertTrue(config.getSinks().get(0).getConfig().getFormat() instanceof JsonLineFormat.Configuration);
        assertEquals("local",config.getSinks().get(0).getName());
    }

    @Test
    public void testSettings() {
        SqrlSettings settings = getDefaultSettings();
        Environment env = Environment.create(settings);
        assertNotNull(env.getDatasetRegistry());
        env.close();
    }

    @Test
    public void testDatasetRegistry() throws InterruptedException {
        SqrlSettings settings = getDefaultSettings(false);
        Environment env = Environment.create(settings);
        DatasetRegistry registry = env.getDatasetRegistry();
        assertNotNull(registry);

        String dsName = "bookclub";
        DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
                .uri(DATA_DIR.toAbsolutePath().toString())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSource(dsName, fileConfig, errors);
        assertFalse(errors.isFatal());
        SourceDataset ds = registry.getDataset(dsName);
        assertNotNull(ds);
        Set<String> tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("book","person"), tablenames);
        assertTrue(ds.containsTable("person"));
        assertNotNull(ds.getTable("book"));
        assertNotNull(ds.getDigest().getCanonicalizer());
        assertEquals(dsName,ds.getDigest().getName().getCanonical());

        env.close();

        //Test that registry correctly persisted tables
        env = Environment.create(settings);
        registry = env.getDatasetRegistry();

        ds = registry.getDataset(dsName);
        assertNotNull(ds);
        tablenames = ds.getTables().stream().map(SourceTable::getName)
                .map(Name::getCanonical).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("book","person"), tablenames);
        env.close();
    }

    @Test
    public void testDatasetMonitoring() throws InterruptedException {
        SqrlSettings settings = getDefaultSettings(true);
        Environment env = Environment.create(settings);
        DatasetRegistry registry = env.getDatasetRegistry();

        String dsName = "bookclub";
        DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
                .uri(DATA_DIR.toAbsolutePath().toString())
                .build();

        ErrorCollector errors = ErrorCollector.root();
        registry.addOrUpdateSource(dsName, fileConfig, errors);
        if (errors.isFatal()) System.out.println(errors);
        assertFalse(errors.isFatal());

        //Needs some time to wait for the flink pipeline to compile data

        SourceDataset ds = registry.getDataset(dsName);
        SourceTable book = ds.getTable("book");
        SourceTable person = ds.getTable("person");

        SourceTableStatistics stats = book.getStatistics();
        assertNotNull(stats);
        assertEquals(4,stats.getCount());
        assertEquals(5, person.getStatistics().getCount());
        env.close();
    }

    public static void validateConfig(GlobalConfiguration config) {
        ErrorCollector errors = config.validate();

        if (errors.hasErrors()) {
            System.err.println(errors.toString());
        }
        if (errors.isFatal()) throw new IllegalArgumentException("Encountered fatal configuration errors");
    }

    public static SqrlSettings getDefaultSettings() {
        return getDefaultSettings(true);
    }

    public static SqrlSettings getDefaultSettings(boolean monitorSources) {
        GlobalConfiguration config = GlobalConfiguration.builder()
                .engines(GlobalConfiguration.Engines.builder()
                        .jdbc(JDBCConfiguration.builder()
                                .dbURL(jdbcURL)
                                .driverName("org.h2.Driver")
                                .dialect(JDBCConfiguration.Dialect.H2)
                                .build())
                        .flink(new FlinkConfiguration())
                        .build())
                .environment(EnvironmentConfiguration.builder()
                        .monitorSources(monitorSources)
                        .build())
                .build();
        validateConfig(config);
        return SqrlSettings.fromConfiguration(config);
    }

}
