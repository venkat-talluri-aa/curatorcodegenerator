package com.aa.rac.mod.codegenerator.eventhub;

import com.aa.rac.mod.codegenerator.FileUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.util.StringUtils;

public class EventHubPojoGenerator {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String ehSourcePath = "./src/main/java/com/aa/rac/mod/repository/eventhub/";

  private static final String packageName = "com.aa.rac.mod.repository.eventhub";

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private Map<String, Object> json = new HashMap<>();

  private String unpackedTrim = "UnpackingNestedStringTrimDeserializer";
  private String unpacked = "";

  private String trimmed = "TrimmingDeserializer";

  private Set<String> ehBaseColumnsSet = new HashSet<>(Arrays.asList("A_ENTTYP", "A_TIMSTAMP", "A_USER", "A_JOBUSER"));

  public EventHubPojoGenerator() {

  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
    }
    return this.json;
  }

  public String getEventHubDirectory() {
    return pwd + ehSourcePath.substring(1);
  }

  public String getFullEventHubFilePath(String fileName) {
    return getEventHubDirectory() + fileName;
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addPackageContents(String packageName) throws IOException {
    lines.add("package " + packageName + end + "\n\n");
  }

  public void addImportStatements() throws IOException {
    String imports = "import com.aa.rac.mod.domain.serialization.TrimmingDeserializer;\n" +
        "import com.aa.rac.mod.domain.serialization.UnpackingNestedStringTrimDeserializer;\n" +
        "import com.fasterxml.jackson.annotation.JsonIgnoreProperties;\n" +
        "import com.fasterxml.jackson.annotation.JsonProperty;\n" +
        "import com.fasterxml.jackson.databind.annotation.JsonDeserialize;\n" +
        "import lombok.Getter;\n" +
        "import lombok.NoArgsConstructor;\n" +
        "import lombok.Setter;\n" +
        "import lombok.ToString;";
    lines.add(imports +"\n\n");
  }

  public void addClassAnnotations() throws IOException {
    String annotations = "@JsonIgnoreProperties(ignoreUnknown = true)\n" +
        "@NoArgsConstructor\n" +
        "@ToString(callSuper = true)\n" +
        "@Setter\n" +
        "@Getter\n";
    lines.add(annotations);
  }

  public void addInitialClassTemplate(String className) throws IOException {
    lines.add("public class " + className + " extends AbstractEventHubBaseEntity { \n\n");
  }

  public Set<String> getFieldNames(String jsonString) throws JsonProcessingException {
    return getJson(jsonString).keySet();
  }

  public String getJsonPropertyAnnotation(String field) {
    return "@JsonProperty(\"" + field + "\")\n";
  }

  public String getJsonDeserializeAnnotation(String className) {
    return "@JsonDeserialize( using = " + className + ".class)\n";
  }

  public void addFields() {
    for (Map.Entry<String, Object> entry: json.entrySet()) {
      String field = entry.getKey();
      String value = entry.getValue().toString();
      if (ehBaseColumnsSet.contains(field)) {
        continue;
      }
      lines.add("  " + getJsonPropertyAnnotation(field));
      if (value.contains("{")) {
        lines.add("  " + getJsonDeserializeAnnotation(unpackedTrim));
      } else {
        lines.add("  " + getJsonDeserializeAnnotation(trimmed));
      }
      lines.add("  private String " + FileUtil.getFieldName(field) +";\n\n");
    }
  }

  public void addEndingLine() {
    lines.add("}");
  }

  public void eventHubPojoFileGenerator(String filePath) throws IOException {
    String eventHubClassName = FileUtil.getClassFileName(filePath);
    Set<String> fieldNames = getFieldNames(String.join("", FileUtil.readLinesFromFile(filePath)));
    String fullPath = getFullEventHubFilePath(eventHubClassName);
    FileUtil.createFile(getEventHubDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addPackageContents(packageName);
    addImportStatements();
    addClassAnnotations();
    addInitialClassTemplate(eventHubClassName.replace(".java.txt", ""));
    addFields();
    addEndingLine();
    try {
      writer.write(String.join("", lines));
    } finally {
      writer.close();
    }
    System.out.println("EventHub Pojo successfully generated. Please review at location: " + fullPath);
  }
}
