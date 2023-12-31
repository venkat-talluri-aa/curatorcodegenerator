package com.aa.rac.mod.codegenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.util.StringUtils;

public class ServiceFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/main/java/com/aa/rac/mod/service/";

  private static final String packageName = "com.aa.rac.mod.service.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String repositoryClassName;

  private String serviceClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  public ReplicatedFileGenerator replicatedFileGenerator;

  public EventHubPojoGenerator eventHubPojoGenerator;

  public RepositoryFileGenerator repositoryFileGenerator;

  private String generatedOutput;

  public String serviceClassMapper;
  public String eventHubClassMapper;
  public String replicatedClassMapper;

  public ServiceFileGenerator(ReplicatedFileGenerator replicatedFileGenerator,
                              EventHubPojoGenerator eventHubPojoGenerator,
                              RepositoryFileGenerator repositoryFileGenerator) {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.repositoryFileGenerator = repositoryFileGenerator;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.repositoryClassName = repositoryFileGenerator.getRepositoryImportPath().substring(repositoryFileGenerator.getRepositoryImportPath().lastIndexOf('.')+1);
    this.serviceClassName = this.replicatedClassName + "ServiceImpl";
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getServiceDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getServiceImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + serviceClassName;
  }

  public String getFullServiceFilePath() {
    return getServiceDirectory() + "/" + serviceClassName + ".java.txt";
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) {
    lines.add("package " + packageName + replicatedClassName.toLowerCase() + end + "\n\n");
  }

  public void addImportStatements(String eventHubImportPath,
                                  String replicatedImportPath,
                                  String repositoryImportPath) throws IOException {
    String imports = "import com.aa.rac.mod.domain.AbstractServiceEventHub;\n" +
        "import com.aa.rac.mod.domain.annotations.SetAuditColumns;\n" +
        "import com.aa.rac.mod.domain.annotations.SetServiceClasses;\n" +
        "import com.aa.rac.mod.domain.enums.CuratedEntityClassMapper;\n" +
        "import com.aa.rac.mod.domain.enums.EventHubPojoClassMapper;\n" +
        "import com.aa.rac.mod.domain.enums.ServiceClassMapper;\n" +
        "import com.aa.rac.mod.domain.exceptions.ProcessingException;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import " + repositoryImportPath + ";\n" +
        "import java.time.LocalDateTime;\n" +
        "import org.springframework.beans.factory.annotation.Autowired;\n" +
        "import org.springframework.beans.factory.annotation.Qualifier;\n" +
        "import org.springframework.scheduling.annotation.Async;\n" +
        "import org.springframework.scheduling.annotation.EnableAsync;\n" +
        "import org.springframework.stereotype.Service;\n";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + replicatedClassName + " service implemenation. */\n");
  }

  public String getClassAnnotations() {
    return "@Service\n" +
        "@EnableAsync\n" +
        "@SetAuditColumns(targetCuratedCdcTimestampField = \"eventHubTimestamp\")\n" +
        "@SetServiceClasses(eventHubClassMapper = EventHubPojoClassMapper." + eventHubClassName.toUpperCase() + ",\n" +
        "    curatedTargetClassMapper = CuratedEntityClassMapper." + replicatedClassName.toUpperCase() + ",\n" +
        "    repoClass = " + repositoryClassName + ".class\n" +
        ")\n";
  }

  public void addInitialClassTemplate(String className) {
    lines.add(getClassAnnotations());
    lines.add("public class " + serviceClassName
        + " \n    extends AbstractServiceEventHub<" + eventHubClassName + ", "
        + replicatedClassName + ", " + repositoryClassName + "> {\n");
  }

  public String getMethodAnnotation() {
    return "\n  @Override\n" + "  @Async";
  }

  public void addMethod(boolean isException) {
    String parameter;
    String retry;
    String payload;
    String serviceClassMapper;
    String eventHubClassMapper;
    String replicatedClassMapper;
    if (isException) {
      parameter = "ProcessingException processingException";
      retry = "processingException.getRetryCount() + 1";
      payload = "processingException.getPayload()";
      serviceClassMapper = "processingException.getCurator()";
      eventHubClassMapper = "processingException.getEventHubSource()";
      replicatedClassMapper = "processingException.getCuratedTarget()";
    } else {
      parameter = "String topicPayload";
      retry = "1";
      payload = "topicPayload";
      serviceClassMapper = "ServiceClassMapper."+ replicatedClassName.toUpperCase() +"_SERVICE_IMPL";
      eventHubClassMapper = "EventHubPojoClassMapper." + eventHubClassName.toUpperCase();
      replicatedClassMapper = "CuratedEntityClassMapper." + replicatedClassName.toUpperCase();
      this.serviceClassMapper = serviceClassMapper;
      this.eventHubClassMapper = eventHubClassMapper;
      this.replicatedClassMapper = replicatedClassMapper;
    }
    lines.add("  " + getMethodAnnotation());
    lines.add("\n  public void processAsync("+ parameter + ") throws ProcessingException { \n");
    lines.add("    try {\n" +
        "      processToCuratedAndEmit("+ payload + ");\n" +
        "    } catch (ProcessingException e) {\n" +
        "      e.setCurator("+ serviceClassMapper +");\n" +
        "      e.setEventHubSource(" + eventHubClassMapper + ");\n" +
        "      e.setCuratedTarget(" + replicatedClassMapper + ");\n" +
        "      e.setPayload("+payload +");\n" +
        "      e.setRetryCount("+ retry +");\n" +
        "      e.setExceptionTimestamp(LocalDateTime.now());\n" +
        "      throw e;\n" +
        "    }\n  }\n");


  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateConverterFile() throws IOException {
    String fullPath = getFullServiceFilePath();
    FileUtil.createFile(getServiceDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(eventHubPojoGenerator.getEventHubImportPath(),
        replicatedFileGenerator.getReplicatedImportPath(),
        repositoryFileGenerator.getRepositoryImportPath());
    addClassJavaDoc();
    addInitialClassTemplate(serviceClassName);
    addMethod(false);
    addMethod(true);
    addEndingLine();
    this.generatedOutput = String.join("", lines);
//    System.out.println(this.generatedOutput);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Service file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
