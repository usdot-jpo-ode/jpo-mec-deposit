package us.dot.its.jpo.ode.mec.deposit.config;

/**
 * Interface for the SystemConfigMBean, which provides methods for configuring the system.
 */
public interface SystemConfigMBean {

  /**
   * Gets the number of threads for the system.
   *
   * @return The number of threads
   */
  public int getThreadCount();

  /**
   * Sets the number of threads for the system.
   *
   * @param noOfThreads The number of threads to set
   */
  public void setThreadCount(int noOfThreads);

  /**
   * Gets the schema name for the system.
   *
   * @return The schema name
   */
  public String getSchemaName();

  /**
   * Sets the schema name for the system.
   *
   * @param schemaName The schema name to set
   */
  public void setSchemaName(String schemaName);

  /**
   * Performs a configuration operation.
   *
   * @return A configuration string
   */
  public String doConfig();
}
