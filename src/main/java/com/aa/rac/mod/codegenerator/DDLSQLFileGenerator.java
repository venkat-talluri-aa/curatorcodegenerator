package com.aa.rac.mod.codegenerator;

import com.aa.rac.mod.CuratorcodegeneratorApplication;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

  private Map<String, String> dataTypeMap = new HashMap<>();

  private Map<String, String> nullMap = new HashMap<>();

  private Map<String, Boolean> trimMap = new HashMap<>();

  private Map<String, String> db2ToPgdataTypeMap = Map.ofEntries(
      Map.entry("CHAR", "VARCHAR"),
      Map.entry("DATE", "DATE"),
      Map.entry("TIMESTMP", "TIMESTAMP WITHOUT TIME ZONE"),
      Map.entry("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"),
      Map.entry("VARCHAR", "VARCHAR"),
      Map.entry("DECIMAL", "DECIMAL"),
      Map.entry("SMALLINT", "SMALLINT"),
      Map.entry("BIGINT", "BIGINT"),
      Map.entry("INTEGER", "INTEGER"),
      Map.entry("INT", "INTEGER")
  );

  private Set<String> db2DataTypeSet = db2ToPgdataTypeMap.keySet();

  private String filePath;

  private String generatedOutput;

  public DDLSQLFileGenerator(String filePath, String tableName) {
    this.filePath = filePath;
    this.tableName = tableName;
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public Map<String, String> getDataTypeMap() {
    return dataTypeMap;
  }

  public Map<String, String> getNullMap() {
    return nullMap;
  }

  public Map<String, Boolean> getTrimMap() {
    return trimMap;
  }

  public void loadDataTypeAndNullAndTrimMaps() {
    if (dataTypeMap.isEmpty()) {
      for (Map.Entry<String, Object> entry: json.entrySet()) {
        String field = entry.getKey();
        String[] properties = entry.getValue().toString().split("[|]");
        String nullable = properties.length>1?properties[1]:"";
        String value = properties[0];
        dataTypeMap.put(field, value);
        nullMap.put(field, nullable);
        Boolean trimmable = properties.length>2?properties[2].equalsIgnoreCase("y"):false;
        trimMap.put(field, trimmable);
      }
    }
  }

  public Map<String, Object> getJson(String jsonString) throws JsonProcessingException {
    if (this.json.isEmpty()) {
      this.json = FileUtil.mapContentsToHashMap(jsonString);
      loadDataTypeAndNullAndTrimMaps();
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

  public String getAuditColumns() {
    Map<String, String> auditColumnsMap = new LinkedHashMap<>();
    auditColumnsMap.put("src_deleted_indicator", "BOOLEAN");
    auditColumnsMap.put("deleted_indicator", "BOOLEAN");
    auditColumnsMap.put("dml_flg", "VARCHAR(3)");
    auditColumnsMap.put("eventhub_timestamp", "TIMESTAMP WITHOUT TIME ZONE");
    auditColumnsMap.put("system_modified_timestamp", "TIMESTAMP WITHOUT TIME ZONE");
    auditColumnsMap.put("created_by", "VARCHAR(100)");
    auditColumnsMap.put("modified_by", "VARCHAR(100)");
    String auditColumns = "";
    for (String name: auditColumnsMap.keySet()) {
      auditColumns += "    "+name + " ".repeat(fieldDataTypeGapLength-name.length())
          + auditColumnsMap.get(name) + " NOT NULL" +", \n";
    }
    return auditColumns;
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
        throw new IllegalArgumentException("Data type not matched. Please check the data types for " + field + ": " + entry.getValue().toString());
      }
      String space = nullable.isBlank()?"":" ";
      lines.add("    "+field + " ".repeat(fieldDataTypeGapLength-field.length())
          + db2ToPgdataTypeMap.get(dataType) + (value.lastIndexOf("(")==-1 || value.toLowerCase().startsWith("timest")?"":value.substring(value.lastIndexOf("("))) + space + nullable +", \n");
    }
    lines.add(getAuditColumns());

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
    this.generatedOutput = String.join("", lines);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("DDL Script file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated script.");
  }
}
