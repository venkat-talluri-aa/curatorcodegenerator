package com.aa.rac.mod.codegenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopicProcessorFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/main/java/com/aa/rac/mod/resources/";

  private static final String packageName = "com.aa.rac.mod.resources.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String repositoryClassName;

  private String processorClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private ReplicatedFileGenerator replicatedFileGenerator;

  private String generatedOutput;

  public TopicProcessorFileGenerator(ReplicatedFileGenerator replicatedFileGenerator) {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.eventHubClassName = this.replicatedClassName.replace("Repl", "");
    this.processorClassName = this.eventHubClassName + "TopicProcessor";
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getServiceDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getServiceImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + processorClassName;
  }

  public String getFullServiceFilePath(String fileName) {
    return getServiceDirectory() + "/" + processorClassName + ".java.txt";
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) {
    lines.add("package " + packageName.substring(0, packageName.length()-1) + end + "\n\n");
  }

  public void addImportStatements(String replicatedImportPath) throws IOException {
    String imports = "import com.aa.rac.mod.domain.BaseService;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import java.util.function.Consumer;\n" +
        "import org.springframework.beans.factory.annotation.Autowired;\n" +
        "import org.springframework.beans.factory.annotation.Qualifier;\n" +
        "import org.springframework.beans.factory.annotation.Value;\n" +
        "import org.springframework.context.annotation.Bean;\n" +
        "import org.springframework.messaging.Message;\n" +
        "import org.springframework.stereotype.Component;\n";
    lines.add(imports +"\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + eventHubClassName + " topic processor. */\n");
  }

  public String getClassAnnotations() {
    return "@Component\n";
  }

  public void addInitialClassTemplate(String className) {
    lines.add(getClassAnnotations());
    lines.add("public class " + processorClassName
        + " \n    extends AbstractTopicProcessor {\n");
  }

  public void addFields() {
    lines.add("\n  @Autowired \n  BaseService<" + replicatedClassName + "> service;\n");
    lines.add("\n  @Value(#TODO) \n  String topicName;\n");
  }

  public void addMethods() {
    lines.add("\n  /**\n" +
        "   * Recieve " + eventHubClassName + " consumer.\n" +
        "   *\n" +
        "   * @return the consumer\n" +
        "   */");
    lines.add("\n  @Bean\n" +
        "  public Consumer<Message<String>> receive"+eventHubClassName+"() {\n" +
        "    return consume(topicName);\n" +
        "  }\n");
    lines.add("\n  @Override\n" +
        "  public BaseService getService() {\n" +
        "    return service;\n" +
        "  }\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateConverterFile() throws IOException {
    String processorClassFileName = processorClassName + ".java.txt";
    String fullPath = getFullServiceFilePath(processorClassFileName);
    FileUtil.createFile(getServiceDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(replicatedFileGenerator.getReplicatedImportPath());
    addClassJavaDoc();
    addInitialClassTemplate(processorClassName);
    addFields();
    addMethods();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
//    System.out.println(this.generatedOutput);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("TopicProcessor file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
