# NetCDF Processor

NetCDF Processor is a demo project to download NetCDF file from AWS S3 buckets whenever file arrival notified is available. Then, the processor read and extract the file to the user database.

## Setup

```bash
mvn install -Dmaven.test.skip=true
```
Set proper values in [AppConfig.java]() (Or replace it by provided)

## Run
* Run through IDE by running one of demo in the main function of NetCDFProcessDemo.java
* Run through a command line,
```bash
to be added
```

## Unknown Issues
1. When running the demo in IDEA, it shows *Error running 'ServiceStarter': Command line is too long. Shorten command line for ServiceStarter or also for Application default configuration.*. The resolution is modify *.idea\workspace.xml* file and look for the tag ```<component name="PropertiesComponent">```. Then add line ```  <property name="dynamic.classpath" value="true" />```