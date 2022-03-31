package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.*;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class CSVFormat implements TextLineFormat<CSVFormat.Configuration> {

    public static final FileFormat FORMAT = FileFormat.CSV;
    public static final String NAME = "csv";

    @Override
    public Parser getParser(Configuration config) {
        return new CSVFormatParser(config);
    }


    @NoArgsConstructor
    public static class CSVFormatParser implements TextLineFormat.Parser {

        private String[] header;
        private String delimiter;
        private String commentPrefix;

        private transient Splitter splitter;

        public CSVFormatParser(Configuration config) {
            Preconditions.checkArgument(config.header!=null && config.header.length>0);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(config.delimiter));
            this.header = config.header;
            this.delimiter = config.delimiter;
            this.commentPrefix = config.commentPrefix;
        }


        @Override
        public Result parse(@NonNull String line) {
            if (isComment(line,commentPrefix)) return Result.skip();
            if (splitter == null) splitter = getSplitter(delimiter);
            List<String> parts = splitter.splitToList(line);
            if (parts.size()>header.length) return Result.error(String.format("Expected %d items per row but found %d",header.length,parts.size()));
            //Skip if line is equal to header
            boolean isHeader = true;
            for (int i = 0; i < parts.size(); i++) {
                if (!parts.get(i).equalsIgnoreCase(header[i])) isHeader = false;
            }
            if (isHeader) return Result.skip();
            HashMap<String,Object> map = new HashMap<>(parts.size());
            for (int i = 0; i < parts.size(); i++) {
                map.put(header[i],parts.get(i));
            }
            return Result.success(map);
        }
    }

    private static Splitter getSplitter(String delimiter) {
        return Splitter.on(delimiter).trimResults();
    }

    private static boolean isComment(String line, String commentPrefix) {
        return !Strings.isNullOrEmpty(commentPrefix) && line.startsWith(commentPrefix);

    }

    @Override
    public Optional<Configuration> getDefaultConfiguration() {
        return Optional.empty();
    }

    @Override
    public Optional<ConfigurationInference<CSVFormat.Configuration>> getConfigInferer() {
        return Optional.of(new Inferer());
    }

    public static class Inferer implements TextLineFormat.ConfigurationInference<Configuration> {

        private Splitter splitter;
        private String[] header;

        public Inferer() {
            this.splitter = getSplitter(DEFAULT_DELIMITER);
        }

        @Override
        public double getConfidence() {
            return header==null?0:0.97;
        }


        @Override
        public Optional<Configuration> getConfiguration() {
            if (header==null || header.length==0) return Optional.empty();
            return Optional.of(Configuration.builder()
                    .header(header).build());
        }

        @Override
        public void nextSegment(@NonNull BufferedReader textInput) throws IOException {
            if (header != null) return; //We already verified there is no header

            String line = textInput.readLine();
            List<String> h = splitter.splitToList(line);
            if (!h.isEmpty()) {
                //verify all header elements are proper strings
                boolean allProper = true;
                for (String s : h) {
                    if (s.isEmpty() || !Character.isLetter(s.charAt(0))) {
                        allProper = false;
                    }
                }
                if (allProper) {
                    header = h.toArray(new String[h.size()]);
                }
            }
            if (header==null) header = new String[0]; //Signal there is no header
        }
    }

    public static final String DEFAULT_DELIMITER = ",";

    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Builder
    @Getter
    public static class Configuration implements FormatConfiguration {

        @NonNull @Builder.Default @NotNull @NotEmpty
        private String delimiter = DEFAULT_DELIMITER;
        private String commentPrefix;
        @NonNull @NotNull @NotEmpty
        private String[] header;

        public static Configuration getDefault() {
            return builder().build();
        }

        @Override
        public boolean validate(ErrorCollector errors) {
            if (Strings.isNullOrEmpty(delimiter)) {
                errors.fatal("Need to specify valid delimiter, given: %s",delimiter);
                return false;
            }
            if (header == null || header.length==0) {
                errors.fatal("Need to specify a header");
                return false;
            }
            return true;
        }

        @Override
        public FileFormat getFileFormat() {
            return FORMAT;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

}
