package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.canonicalizer.NamePath;
import lombok.Value;

@Value
public class SqrlExportStatement implements SqrlDdlStatement {

  ParsedObject<NamePath> tableIdentifier;
  ParsedObject<NamePath> packageIdentifier;

}
