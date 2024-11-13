/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

@AllArgsConstructor
public class SessionAggregationHint implements SqrlHint {

  //TODO ECH: what is the instant aggregation type ?
  public enum Type {FUNCTION, INSTANT}

  @Getter
  final int windowFunctionIdx;
  @Getter
  final Type type;
  @Getter
  final int inputTimestampIdx;
  @Getter
  final long windowGapMs;

  public static SessionAggregationHint instantOf(int timestampIdx) {
    return new SessionAggregationHint(timestampIdx, Type.INSTANT, timestampIdx, 1);
  }

  public static SessionAggregationHint functionOf(int windowFunctionIdx, int inputTimestampIdx, long windowGapMs) {
    return new SessionAggregationHint(windowFunctionIdx, Type.FUNCTION, inputTimestampIdx, windowGapMs);
  }

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(List.of(String.valueOf(windowFunctionIdx), String.valueOf(type),
            String.valueOf(inputTimestampIdx), String.valueOf(windowGapMs))).build();
  }

  public static final String HINT_NAME = SessionAggregationHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<SessionAggregationHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public SessionAggregationHint fromHint(RelHint hint) {
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 4, "Invalid hint: %s", hint);
      return new SessionAggregationHint(Integer.valueOf(options.get(0)),
          Type.valueOf(options.get(1)),
          Integer.valueOf(options.get(2)), Long.valueOf(options.get(3)));
    }
  }

}
