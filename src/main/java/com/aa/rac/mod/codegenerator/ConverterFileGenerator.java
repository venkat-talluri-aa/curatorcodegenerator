package com.aa.rac.mod.codegenerator;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.springframework.util.StringUtils;

public class ConverterFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/main/java/com/aa/rac/mod/domain/converter/";

  private static final String packageName = "com.aa.rac.mod.domain.converter.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String converterClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private ReplicatedFileGenerator replicatedFileGenerator;

  private EventHubPojoGenerator eventHubPojoGenerator;

  private String replicatedClassFilepath;

  private List<String> replClassAllLines;

  public ConverterFileGenerator(ReplicatedFileGenerator replicatedFileGenerator, EventHubPojoGenerator eventHubPojoGenerator, String replicatedClassFilepath)
      throws IOException {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.replicatedClassFilepath = replicatedClassFilepath;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.converterClassName = this.eventHubClassName + "To" + this.replicatedClassName + "Converter";
    this.replClassAllLines = Files.readAllLines(Paths.get(replicatedClassFilepath));
  }

  public void getReplicatedFieldNames() {

  }

  public String getConverterDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getConverterImportPath() {
    return packageName + converterClassName;
  }

  public String getFullConverterFilePath(String fileName) {
    return getConverterDirectory() + "/" + converterClassName + ".java.txt";
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) {
    lines.add("package " + packageName + end + "\n\n");
  }

  public void addImportStatements(String eventHubImportPath, String replicatedImportPath) throws IOException {
    String imports = "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import java.math.RoundingMode;\n" +
        "import java.sql.Timestamp;\n" +
        "import org.jetbrains.annotations.NotNull;\n" +
        "import org.springframework.core.convert.converter.Converter;";
    lines.add(imports +"\n\n");
  }

  public void addInitialClassTemplate(String className) {
    lines.add("public class " + converterClassName
        + " implements Converter<" + eventHubClassName + ", "
        + replicatedClassName + "> {\n\n");
  }

  public String getMethodAnnotation() {
    return "@Override";
  }

  public void addMethod() {
    lines.add("  " + getMethodAnnotation());
    lines.add("\n  public " + replicatedClassName + " convert(@NotNull " + eventHubClassName + " source) {\n    "
    + replicatedClassName + " target = new " + replicatedClassName + "();\n    try { \n      switch (source.getEntityType()) {\n        case \"PT\", \"UP\", \"DL\" -> {");
    //#TODO
    lines.add("}\n" +
        "        default -> throw new IllegalStateException(\n" +
        "            \"Unexpected value: \" + source.getEntityType());\n" +
        "      }\n" +
        "    } catch (Exception ex) {\n" +
        "      throw new RuntimeException(ex);\n" +
        "    }\n" +
        "    return target;\n" +
        "  }\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateConverterFile() throws IOException {
    String converterClassFileName = converterClassName + ".java.txt";
    String fullPath = getFullConverterFilePath(converterClassFileName);
    FileUtil.createFile(getConverterDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(eventHubPojoGenerator.getEventHubImportPath(), replicatedFileGenerator.getReplicatedImportPath());
    addInitialClassTemplate(converterClassName);
    addMethod();
    addEndingLine();
    System.out.println(String.join("", lines));
//    try {
//      writer.write(String.join("", lines));
//    } finally {
//      writer.close();
//    }
    System.out.println("Repository file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
