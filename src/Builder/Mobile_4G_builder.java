package Builder;

import Model.Mobile_4G_LogEntity;

public class Mobile_4G_builder extends DpiBuilder {

  Mobile_4G_LogEntity entity = null;
  public static Mobile_4G_builder builder = null;

  @Override
  public Mobile_4G_LogEntity parseLineToEntity(String line) {
    // TODO Auto-generated method stub
    entity = new Mobile_4G_LogEntity();
    String[] splits = line.split("\\|");
    if (splits.length <= 30) {
      return null;
    }
    entity.setUrl(splits[29]);
    entity.setMismdn(splits[1]);
    return this.entity;
  }

  public static Mobile_4G_builder getInstance() {
    if (builder == null) {
      builder = new Mobile_4G_builder();
    }
    return builder;
  }

  @Override
  public Mobile_4G_LogEntity build(String line) {
    // TODO Auto-generated method stub
    return parseLineToEntity(line);
  }


}
