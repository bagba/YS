package Builder;

import Model.DpiEntity;

public abstract class DpiBuilder {

  public abstract DpiEntity build(String line);
  public abstract DpiEntity parseLineToEntity(String line);
  
}
