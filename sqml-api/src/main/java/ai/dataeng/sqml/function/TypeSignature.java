package ai.dataeng.sqml.function;

import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.Type;

public class TypeSignature {

  private final String name;
  private final Type type;

  public TypeSignature(String name, Type type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }
}
