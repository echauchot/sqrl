/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.Build;
import com.datasqrl.service.PackagerUtil;
import com.datasqrl.service.PathUtil;
import com.datasqrl.service.Util;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

  public static final String DEFAULT_DEPLOY_DIR = "deploy";
  public static final String DEFAULT_SERVER_MODEL = "model.json";
  public static final String DEFAULT_SERVER_CONFIG = "config.json";

  private final boolean execute;
  private final boolean startGraphql;

  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) GraphQL schema")
  private Path[] files;

  @CommandLine.Option(names = {"-s", "--schema"}, description = "Generates the graphql "
      + "schema file and exits")
  private boolean generateSchema = false;

  @CommandLine.Option(names = {"-o", "--output-dir"}, description = "Output directory")
  private Path outputDir = null;

  @CommandLine.Option(names = {"-p", "--port"}, description = "Port for API server")
  private int port = 8888;

  private final ObjectWriter writer = new ObjectMapper()
      .writerWithDefaultPrettyPrinter();

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql) {
    this.execute = execute;
    this.startGraphql = startGraphql;
  }

  @SneakyThrows
  public void runCommand(ErrorCollector collector) throws IOException {
    List<Path> packageFiles = PathUtil.getOrCreateDefaultPackageFiles(root);

    Build build = new Build(collector);
    Packager packager = PackagerUtil.create(root.rootDir, files, packageFiles);
    Path buildLoc = build.build(packager);

    Compiler compiler = new Compiler();
    Compiler.CompilerResult result = compiler.run(collector, buildLoc);

    GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.readFrom(packageFiles,
        GlobalEngineConfiguration.class);
    JdbcDataSystemConnectorConfig jdbc = Util.getJdbcEngine(engineConfig.getEngines());


    if (generateSchema) {
      writeSchema(result);
      return;
    } else {
      write(result, jdbc);
    }

    Optional<CompletableFuture> fut = Optional.empty();
    if (startGraphql) {
      fut = Optional.of(CompletableFuture.runAsync(()->
          startGraphQLServer(result.getModel(),
              port, jdbc)));
    }

    if (execute) {
      executePlan(result.getPlan());
    }

    fut.map(f-> {
      try {
        return f.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @SneakyThrows
  private void writeSchema(CompilerResult result) {
    Files.writeString(Path.of(Packager.GRAPHQL_SCHEMA_FILE_NAME),
        result.getGraphQLSchema(), StandardOpenOption.CREATE);
  }

  @SneakyThrows
  private void write(CompilerResult result, JdbcDataSystemConnectorConfig jdbc) {
    if (outputDir == null) {
      outputDir = root.rootDir.resolve(DEFAULT_DEPLOY_DIR);
    }
    if (Files.isDirectory(outputDir)) {
      FileUtils.cleanDirectory(outputDir.toFile());
    } else {
      Files.createDirectories(outputDir);
    }
    writeTo(result, outputDir);
    writeTo(jdbc, outputDir);
  }

  private void writeTo(JdbcDataSystemConnectorConfig jdbc, Path outputDir) throws IOException {
    Files.writeString(outputDir.resolve(DEFAULT_SERVER_CONFIG),
        writer.writeValueAsString(jdbc), StandardOpenOption.CREATE);
  }

  public void writeTo(CompilerResult result, Path outputDir) throws IOException {
    Files.writeString(outputDir.resolve(DEFAULT_SERVER_MODEL),
        writer.writeValueAsString(result.getModel()), StandardOpenOption.CREATE);
  }

  private void executePlan(PhysicalPlan physicalPlan) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan);
  }
}
