package com.aa.rac.mod.codegenerator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.util.StringUtils;

public class RepositoryFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/main/java/com/aa/rac/mod/repository/findatahub/";

  private static final String packageName = "com.aa.rac.mod.repository.findatahub.";

  private String replicatedClassName;

  private String repositoryClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  public RepositoryFileGenerator(String replicatedClassName) {
    this.replicatedClassName = replicatedClassName;
    this.repositoryClassName = this.replicatedClassName + "Repository";
  }

  public String getRepositoryDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getRepositoryImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + repositoryClassName;
  }

  public String getFullRepositoryFilePath(String fileName) {
    return getRepositoryDirectory() + replicatedClassName.toLowerCase() + "/" + repositoryClassName + ".java.txt";
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) throws IOException {
    lines.add("package " + packageName + replicatedClassName.toLowerCase() + end + "\n\n");
  }

  public void addImportStatements(String replicatedImportPath) throws IOException {
    String imports = "import " + replicatedImportPath + ";\n" +
        "import java.util.Optional;\n" +
        "import org.springframework.data.jpa.repository.JpaRepository;\n" +
        "import org.springframework.data.jpa.repository.Query;\n" +
        "import org.springframework.stereotype.Repository;";
    lines.add(imports +"\n\n");
  }

  public void addClassAnnotations() throws IOException {
    String annotations = "@Repository\n";
    lines.add(annotations);
  }

  public void addInitialClassTemplate(String className) throws IOException {
    lines.add("public class " + className
        + " extends JpaRepository<"
        + replicatedClassName + ", String>" + " { \n\n");
  }
//@Query(
//      value = "SELECT * FROM curated.processed_refund WHERE refunded_amount_uuid=?1",
//      nativeQuery = true)
//  Optional<ProcessedRefund> findByRefundedAmountUuid(String refundedAmountUuid);

  public String getQueryAnnotation(String uuidColumnName) {
    return "@Query( value = \"SELECT * FROM curated_test."
        + replicatedClassName.replace("Repl", "").toLowerCase()
        + "\"\n                + \" WHERE " + uuidColumnName + "=?1\", " +
        "\n                  nativeQuery = true)\n";
  }

  public void addMethod(String uuidColumnName) {
    lines.add("  " + getQueryAnnotation(uuidColumnName));
    String fieldName = FileUtil.getFieldName(uuidColumnName);
    lines.add("  " + "Optional<"+replicatedClassName+"> findBy"
        + StringUtils.capitalize(fieldName)
        + "(String " + fieldName + ");\n");
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateRepositoryFile(String replicatedImportPath, String uuidColumnName) throws IOException {
    String repositoryClassFileName = repositoryClassName + ".java.txt";
    String fullPath = getFullRepositoryFilePath(repositoryClassFileName);
    FileUtil.createFile(getRepositoryDirectory() + replicatedClassName.toLowerCase(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements(replicatedImportPath);
    addClassAnnotations();
    addInitialClassTemplate(repositoryClassName);
    addMethod(uuidColumnName);
    addEndingLine();
    try {
      writer.write(String.join("", lines));
    } finally {
      writer.close();
    }
    System.out.println("Repository file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
