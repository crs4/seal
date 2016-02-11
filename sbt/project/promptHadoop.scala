package it.crs4.tools

object promptHadoop {
  def ask : String = {
    val version = "2.7.1"
    val read = readLine(s"""* Which version of Hadoop do you want to use? (enter = default = $version)
  See http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client for available versions.
  ---> """)
    if (read.isEmpty)
      version
    else
      read
  }
}
