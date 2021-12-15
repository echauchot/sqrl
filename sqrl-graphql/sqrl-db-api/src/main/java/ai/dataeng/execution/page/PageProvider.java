package ai.dataeng.execution.page;

import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Optional;

public interface PageProvider {
  public Object wrap(List<Object> items, String page, boolean hasNextPage);

  //Todo: Transform as hooks
  public boolean hasNextPageAttribute(DataFetchingEnvironment environment);
  Optional<Integer> parsePageSize(DataFetchingEnvironment environment);
  //Todo: page state as object
  Optional<String> pageState(DataFetchingEnvironment environment);
}
