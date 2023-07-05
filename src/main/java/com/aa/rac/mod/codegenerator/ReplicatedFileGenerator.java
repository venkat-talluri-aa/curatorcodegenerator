package com.aa.rac.mod.codegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicatedFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/main/java/com/aa/rac/mod/orm/dao/";

  private static final String packageName = "com.aa.rac.mod.orm.dao.";

  private String tableName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private Map<String, Object> json = new LinkedHashMap<>();

  private String unpackedTrim = "UnpackingNestedStringTrimDeserializer";
  private String unpacked = "";

  private String trimmed = "TrimmingDeserializer";

  private Set<String> ehBaseColumnsSet = new HashSet<>(Arrays.asList("A_ENTTYP", "A_TIMSTAMP", "A_USER", "A_JOBUSER"));

  private String replicatedClassName;

  private String filePath;

  public ReplicatedFileGenerator(String filePath) {
    this.filePath = filePath;
    String eventHubClassName = FileUtil.getClassName(filePath);
    replicatedClassName = eventHubClassName + "Repl";
    this.tableName = eventHubClassName.toLowerCase();
  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
    }
    return this.json;
  }

  public String getReplicatedDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getReplicatedImportPath() {
    return packageName + replicatedClassName.toLowerCase() + "." + replicatedClassName;
  }

  public String getFullReplicatedFilePath(String fileName) {
    return getReplicatedDirectory() + tableName +"repl/" + fileName;
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) throws IOException {
    lines.add("package " + packageName + tableName + "repl" + end + "\n\n");
  }

  public void addImportStatements() throws IOException {
    String imports = "import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;\n" +
        "import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;\n" +
        "\n" +
        "import com.aa.rac.mod.domain.enums.CuratedEntityClassMapper;\n" +
        "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import com.aa.rac.mod.orm.dao.AbstractCuratedBaseEntity;\n" +
        "import com.aa.rac.mod.orm.dao.CuratedDependency;\n" +
        "import com.fasterxml.jackson.annotation.JsonAutoDetect;\n" +
        "import jakarta.persistence.Column;\n" +
        "import jakarta.persistence.Entity;\n" +
        "import jakarta.persistence.Id;\n" +
        "import jakarta.persistence.Table;\n" +
        "import java.math.BigDecimal;\n" +
        "import java.sql.Date;\n" +
        "import java.sql.Timestamp;\n" +
        "import lombok.Getter;\n" +
        "import lombok.NoArgsConstructor;\n" +
        "import lombok.Setter;\n" +
        "import lombok.ToString;";
    lines.add(imports +"\n\n");
  }

  public void addClassAnnotations() throws IOException {
    String annotations = "@Entity\n" +
        "@Table(name = \"" + tableName + "\", schema = \"" + "curated_test" + "\")\n" +
        "@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)\n" +
        "@NoArgsConstructor\n" +
        "@ToString(callSuper = true)\n" +
        "@Setter\n" +
        "@Getter\n";
    lines.add(annotations);
  }

  public void addInitialClassTemplate(String className) throws IOException {
    lines.add("public class " + className + " extends AbstractCuratedBaseEntity { \n\n");
  }

  public Set<String> getFieldNames(String jsonString) throws JsonProcessingException {
    return getJson(jsonString).keySet();
  }

  public String getColumnAnnotation(String field) {
    return "@Column( name = \"" + field + "\")\n";
  }

  public String getIdAnnotation() {
    return "@Id\n";
  }

  public void addFields(String uuidColumnName) {
    lines.add("  " + getIdAnnotation());
    lines.add("  " + getColumnAnnotation(uuidColumnName));
    lines.add("  private String " + FileUtil.getFieldName(uuidColumnName) + ";\n\n");
    for (Map.Entry<String, Object> entry: json.entrySet()) {
      String field = entry.getKey();
      String value = entry.getValue().toString();
      if (ehBaseColumnsSet.contains(field) || field.startsWith("B_")) {
        continue;
      }
      lines.add("  " + getColumnAnnotation(field));
      lines.add("  private String " + FileUtil.getFieldName(field) +";\n\n");
    }
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void generateReplicatedFile(String uuidColumnName) throws IOException {
    String replicatedClassFileName = replicatedClassName + ".java.txt";
    Set<String> fieldNames = getFieldNames(String.join("", FileUtil.readLinesFromFile(filePath)));
    String fullPath = getFullReplicatedFilePath(replicatedClassFileName);
    FileUtil.createFile(getReplicatedDirectory() + tableName +"repl", fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements();
    addClassAnnotations();
    addInitialClassTemplate(replicatedClassName);
    addFields(uuidColumnName);
    addEndingLine();
    try {
      writer.write(String.join("", lines));
    } finally {
      writer.close();
    }
    System.out.println("Replicated file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review uuid column and modify data types of other columns.");
  }
}
