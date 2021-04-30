

## Build

First build the ignite compatible Docker images for nodejs runtime by executing below command in `nodejs` folder

```bash
docker build -t whisk/ignite-nodejs-v12:latest  .
``` 

## Launch Standalone

```
$ ./gradlew :core:standalone:build
$ sudo java -Dwhisk.spi.ContainerFactoryProvider=org.apache.openwhisk.core.containerpool.ignite.IgniteContainerFactoryProvider \
      -jar bin/openwhisk-standalone.jar \
      -m tools/ignite/ignite-runtimes.json
```

-D  vm parameter

-jar
执行该命令时，会用到目录META-INF\MANIFEST.MF文件，
在该文件中，有一个叫Main－Class的参数，它说明了java -jar命令执行的类

