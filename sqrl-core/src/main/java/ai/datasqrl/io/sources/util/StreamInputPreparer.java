package ai.datasqrl.io.sources.util;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.StreamHolder;

public interface StreamInputPreparer {

    boolean isRawInput(SourceTable table);

    StreamHolder<SourceRecord.Raw> getRawInput(SourceTable table, StreamEngine.Builder builder);

//    void importTable(ImportManager.SourceTableImport tableImport, StreamEngine.Builder builder);

}
