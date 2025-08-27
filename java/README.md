### compile:
mvn clean compile

### Run:
mvn exec:java -Dexec.mainClass="org.kv.writer.Main"

### Deploy
部署配置，可参考：https://confluence.shopee.io/display/BIG/beeshop_common+Java+JAR+Management 1、2、3步骤，替换settings.xml 到~/.m2/目录下
mvn clean deploy

