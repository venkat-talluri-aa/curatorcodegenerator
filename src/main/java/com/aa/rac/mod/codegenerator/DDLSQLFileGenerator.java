package com.aa.rac.mod.codegenerator;

import com.aa.rac.mod.CuratorcodegeneratorApplication;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DDLSQLFileGenerator {

  private int fieldDataTypeGapLength = 30;
  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/test/resources/";

  private String tableName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private Map<String, Object> json = new LinkedHashMap<>();

  private Set<String> db2DataTypeSet = new HashSet<>(Arrays.asList("CHAR", "DATE", "TIMESTAMP", "VARCHAR", "DECIMAL", "SMALLINT", "BIGINT", "INTEGER"));

  private String filePath;

  public DDLSQLFileGenerator(String filePath, String tableName) {
    this.filePath = filePath;
    this.tableName = tableName;
  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
    }
    return this.json;
  }

  public String getDDLDirectory() {
    return pwd + replSourcePath.substring(1);
  }

  public String getFullDDLdFilePath(String fileName) {
    return getDDLDirectory() + fileName;
  }

  public FileWriter getFileWriter(String fullFilePath) throws IOException {
    if (fileWriter == null) {
      fileWriter = new FileWriter(fullFilePath);
    }
    return fileWriter;
  }

  public void addCreateStatement() {
    lines.add("CREATE TABLE IF NOT EXISTS curated_test." + tableName +"\n(\n");
  }

  public Set<String> getFieldNames(String jsonString) throws JsonProcessingException {
    return getJson(jsonString).keySet();
  }

  public void addFields(String uuidColumnName) {
    lines.add("    " + uuidColumnName + " ".repeat(fieldDataTypeGapLength-uuidColumnName.length()) + "CHAR(64) NOT NULL, \n");
    for (Map.Entry<String, Object> entry: json.entrySet()) {
      String field = entry.getKey();
      String[] properties = entry.getValue().toString().split("[|]");
      String nullable = properties.length>1?properties[1]:"";
      String value = properties[0];
      String dataType = value.lastIndexOf("(") == -1 ? value : value.substring(0, value.lastIndexOf("("));
      if (!db2DataTypeSet.contains(dataType)) {
        throw new IllegalArgumentException("Data type not matched. Please check the data types.");
      }
      String space = nullable.isBlank()?"":" ";
      lines.add("    "+field + " ".repeat(fieldDataTypeGapLength-field.length())
          + value.replace(",", "") + space + nullable +", \n");
    }
  }

  public void addPKConstraint(String uuidColumnName) {
    lines.add("    CONSTRAINT " + CuratorcodegeneratorApplication.SCHEMA_NAME + "_" +
        uuidColumnName + "_pkey PRIMARY KEY ("+ uuidColumnName + ")\n");
  }

  public void addEndingLine() {
    lines.add(")");
  }

  public void generateDDLFile(String uuidColumnName) throws IOException {
    String ddlFileName = tableName + "_ddl.txt";
    Set<String> fieldNames = getFieldNames(String.join("", FileUtil.readLinesFromFile(filePath)));
    String fullPath = getFullDDLdFilePath(ddlFileName);
    FileUtil.createFile(getDDLDirectory(), fullPath);
    FileWriter writer = getFileWriter(fullPath);
    addCreateStatement();
    addFields(uuidColumnName);
    addPKConstraint(uuidColumnName);
    addEndingLine();
    try {
      writer.write(String.join("", lines));
    } finally {
      writer.close();
    }
    System.out.println("DDL Script file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated script.");
  }
}
