package com.aa.rac.mod.codegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.util.StringUtils;

public class TestFileGenerator {

  private static final String pwd = System.getProperty("user.dir").replace('\\', '/');

  private static final String replSourcePath = "./src/test/java/com/aa/rac/mod/service/";

  private static final String packageName = "com.aa.rac.mod.service.";

  private String eventHubClassName;

  private String replicatedClassName;

  private String repositoryClassName;

  private String serviceClassName;

  private String testClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private ServiceFileGenerator serviceFileGenerator;

  private ReplicatedFileGenerator replicatedFileGenerator;

  private EventHubPojoGenerator eventHubPojoGenerator;

  private RepositoryFileGenerator repositoryFileGenerator;

  private DDLSQLFileGenerator ddlsqlFileGenerator;

  private String generatedOutput;

  private String serviceVariable;
  private String repoVariable;

  private String insertVariable;
  private String updateVariable;
  private String deleteVariable;
  private String replCamel;

  private String uuidColumn;

  private String insertException;

  String insFilepath;
  String updFilepath;
  String delFilepath;

  public Map<String, String> insertContents = new HashMap<>();
  public Map<String, String> updateContents = new HashMap<>();
  public Map<String, String> deleteContents = new HashMap<>();

  public TestFileGenerator(ServiceFileGenerator serviceFileGenerator, DDLSQLFileGenerator ddlsqlFileGenerator, String insFilepath, String updFilepath, String delFilepath)
      throws IOException {
    this.insFilepath = insFilepath;
    this.updFilepath = updFilepath;
    this.delFilepath = delFilepath;
    this.serviceFileGenerator = serviceFileGenerator;
    this.replicatedFileGenerator = serviceFileGenerator.replicatedFileGenerator;
    this.eventHubPojoGenerator = serviceFileGenerator.eventHubPojoGenerator;
    this.repositoryFileGenerator = serviceFileGenerator.repositoryFileGenerator;
    this.ddlsqlFileGenerator = ddlsqlFileGenerator;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.repositoryClassName = repositoryFileGenerator.getRepositoryImportPath().substring(repositoryFileGenerator.getRepositoryImportPath().lastIndexOf('.')+1);
    this.serviceClassName = this.replicatedClassName + "ServiceImpl";
    this.testClassName = this.serviceClassName + "Test";
    this.serviceVariable = this.eventHubClassName.toLowerCase() + "Service";
    this.repoVariable = this.repositoryClassName.substring(0,1).toLowerCase()
        + this.repositoryClassName.substring(1);
    this.insertVariable = "insert"+this.eventHubClassName;
    this.updateVariable = "update"+this.eventHubClassName;
    this.deleteVariable = "delete"+this.eventHubClassName;
    this.replCamel = this.replicatedClassName.substring(0, 1).toLowerCase() + this.replicatedClassName.substring(1);
    this.uuidColumn = StringUtils.capitalize(FileUtil.getFieldName(repositoryFileGenerator.uuidColumnName));
    this.insertException = this.insertVariable +"Exception";
    readFiles();
  }

  public Map<String, String> getJson(String jsonString) throws JsonProcessingException {
    Map<String, String> contents = new HashMap<>();
    for (Map.Entry<String, Object> entry: FileUtil.mapContentsToHashMap(jsonString).entrySet()) {
      String value = entry.getValue().toString();
      if (value.contains("string")) {
        value = value.substring(8, value.length()-1);
      }
      String trimKey = entry.getKey().startsWith("B_")?entry.getKey().substring(2):entry.getKey();
      if (ddlsqlFileGenerator.trimMap.getOrDefault(trimKey, false)) {
        value = value.trim();
      }
      if (entry.getKey().equalsIgnoreCase("A_TIMSTAMP")){
        value = value.substring(0, 26);
      }
      if (entry.getKey().equalsIgnoreCase("TICKET_CREATE_TS") || entry.getKey().equalsIgnoreCase("B_TICKET_CREATE_TS")){
        value = value.substring(0, 20).replace('T', ' ')+"0";
      }
      contents.put(entry.getKey(), value.isBlank()?null:value);
    }
    return contents;
  }

  public void readFiles() throws IOException {
    insertContents = getJson(String.join("", FileUtil.readLinesFromFile(insFilepath)));
    updateContents = getJson(String.join("", FileUtil.readLinesFromFile(updFilepath)));
    deleteContents = getJson(String.join("", FileUtil.readLinesFromFile(delFilepath)));
  }

  public String getGeneratedOutput() {
    return generatedOutput;
  }

  public String getServiceDirectory() {
    return pwd + replSourcePath.substring(1) + "/" + replicatedClassName.toLowerCase() ;
  }

  public String getFullServiceFilePath() {
    return getServiceDirectory() + "/" + testClassName + ".java.txt";
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
    String imports = "import static org.junit.jupiter.api.Assertions.assertEquals;\n" +
        "import static org.junit.jupiter.api.Assertions.assertNotNull;\n" +
        "import static org.junit.jupiter.api.Assertions.assertTrue;\n" +
        "import static org.junit.jupiter.api.Assertions.fail;\n" +
        "import static org.mockito.Mockito.times;\n" +
        "import static org.mockito.Mockito.verify;\n" +
        "\n" +
        "import com.aa.rac.mod.domain.BaseService;\n" +
        "import com.aa.rac.mod.domain.enums.CuratedEntityClassMapper;\n" +
        "import com.aa.rac.mod.domain.enums.EventHubPojoClassMapper;\n" +
        "import com.aa.rac.mod.domain.enums.ExceptionType;\n" +
        "import com.aa.rac.mod.domain.enums.ServiceClassMapper;\n" +
        "import com.aa.rac.mod.domain.exceptions.ProcessingException;\n" +
        "import com.aa.rac.mod.domain.exceptions.ProcessingExceptionHandler;\n" +
        "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import " + repositoryImportPath + ";\n" +
        "import com.aa.rac.mod.util.AbstractTestSupport;\n" +
        "import com.aa.rac.mod.util.TestUtil;\n" +
        "import com.fasterxml.jackson.databind.ObjectMapper;\n" +
        "import java.math.BigInteger;\n" +
        "import java.time.format.DateTimeFormatter;\n" +
        "import java.util.Optional;\n" +
        "import java.util.concurrent.CountDownLatch;\n" +
        "import java.util.concurrent.TimeUnit;\n" +
        "import nl.altindag.log.LogCaptor;\n" +
        "import org.junit.jupiter.api.BeforeEach;\n" +
        "import org.junit.jupiter.api.DisplayName;\n" +
        "import org.junit.jupiter.api.Test;\n" +
        "import org.junit.jupiter.api.extension.ExtendWith;\n" +
        "import org.mockito.Mockito;\n" +
        "import org.mockito.junit.jupiter.MockitoExtension;\n" +
        "import org.springframework.beans.factory.annotation.Autowired;\n" +
        "import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;\n" +
        "import org.springframework.boot.test.context.SpringBootTest;\n" +
        "import org.springframework.boot.test.mock.mockito.MockBean;\n";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + serviceClassName + " H2 test. */\n");
  }

  public String getClassAnnotations() {
    return "@SpringBootTest\n" +
        "@AutoConfigureTestDatabase\n" +
        "@ExtendWith(MockitoExtension.class)\n" +
        "@SuppressWarnings(\"checkstyle:LineLength\")\n";
  }

  public void addInitialClassTemplate(String className) {
    lines.add(getClassAnnotations());
    lines.add("public class " + testClassName +" extends AbstractTestSupport {\n");
  }

  public void addFields() throws IOException {
    lines.add("\n  private final CountDownLatch lock = new CountDownLatch(1);\n" +
        "  private final ObjectMapper mapper = new ObjectMapper();\n" +
        "\n" +
        "  DateTimeFormatter formatter = DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSS\");\n" +
        "\n" +
        "  private LogCaptor logCaptor;\n" +
        "\n" +
        "  @MockBean\n" +
        "  ProcessingExceptionHandler processingExceptionHandler;\n");
    lines.add("\n  @Autowired \n" +
        "  private " + repositoryClassName + " "
        + repoVariable + ";");
    lines.add("\n\n  @Autowired\n  private BaseService<"+eventHubClassName+", "+replicatedClassName+"> " + serviceVariable+";");
    lines.add("\n\n\n  private final String " + insertVariable + " = \""
        + String.join("", FileUtil.readLinesFromFile(insFilepath)).replace("\"", "\\\"")+"\";");
    lines.add("\n\n  private final String " + updateVariable + " = \""
        + String.join("", FileUtil.readLinesFromFile(updFilepath)).replace("\"", "\\\"")+"\";");
    lines.add("\n\n  private final String " + deleteVariable +  " = \""
        + String.join("", FileUtil.readLinesFromFile(delFilepath)).replace("\"", "\\\"")+"\";");
    lines.add("\n\n  private final String " + insertException + " = #TODO;");
  }
  public void addInsertTest() {
    lines.add("  /** Test insert. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"INSERT a new record\")\n" +
        "  public void testInsert() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+insertVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+insertVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "      TestUtil.assertTrueTest(\n"  +
        "          \""+ insertContents.get("A_TIMSTAMP")+"\",\n" +
        "          "+replCamel+".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
        "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
        "\n" +
        "      testInsertData("+replCamel+".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addUpdateTest() {
    lines.add("  /** Test Update. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"UPDATE an existing record\")\n" +
        "  public void testUpdate() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+insertVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+updateVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+insertVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "      TestUtil.assertTrueTest(\n" +
        "          \""+ updateContents.get("A_TIMSTAMP")+"\",\n" +
        "          "+replCamel+".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"UP\", "+replCamel+".get().getDmlFlg());\n" +
        "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
        "\n" +
        "      testUpdateData("+replCamel+".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addConcurrentInsertTest() {
    lines.add("  /** Test concurrent inserts. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Concurrent INSERTS throwing Exception\")\n" +
        "  public void testConcurrentInserts() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+insertVariable+");\n" +
        "      "+serviceVariable+".processAsync("+updateVariable+");\n" +
        "      "+serviceVariable+".processAsync("+deleteVariable+");\n" +
        "      lock.await(5000, TimeUnit.MILLISECONDS);\n" + "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+insertVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "\n" +
        "      TestUtil.assertConcurrentEvents(\n" +
        "          "+replCamel+".get(),\n" +
        "          processingExceptionHandler,\n" +
        "          logCaptor.getErrorLogs(),\n"+
        "          ExceptionType.FAILURE_CONCURRENT_INSERT.name(),\n"+
        "          ExceptionType.FAILURE_CONCURRENT_UPDATE.name());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addConcurrentUpdateTest() {
    lines.add("  /** Test concurrent updates. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Concurrent UPDATES throwing Exception\")\n" +
        "  public void testConcurrentUpdates() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+insertVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n\n" +
        "      "+serviceVariable+".processAsync("+updateVariable+");\n" +
        "      "+serviceVariable+".processAsync("+deleteVariable+");\n" +
        "      lock.await(4000, TimeUnit.MILLISECONDS);\n" + "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+insertVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "\n" +
        "      TestUtil.assertConcurrentEvents(\n" +
        "          "+replCamel+".get(),\n" +
        "          processingExceptionHandler,\n" +
        "          logCaptor.getErrorLogs(),\n" +
        "          ExceptionType.FAILURE_CONCURRENT_UPDATE.name());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addDeleteTest() {
    lines.add("  /** Test delete. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Soft DELETE existing record with src_deleted_indicator\")\n" +
        "  public void testDelete() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+insertVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+deleteVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+insertVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "      TestUtil.assertTrueTest(\n" +
        "          \""+ deleteContents.get("A_TIMSTAMP")+"\",\n" +
        "          "+replCamel+".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"UP\", "+replCamel+".get().getDmlFlg());\n" +
        "      assertEquals(true, "+replCamel+".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
        "\n" +
        "      testDeleteData("+replCamel+".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addOutOfOrderUpdateTest() {
    lines.add("   /**\n" +
        "   * Event out of order with new record coming early.\n" +
        "   * For e.g. UPDATE comes earlier than INSERT.\n" +
        "   */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Out-of-order: new UPDATE record coming early\")\n" +
        "  public void testOutOfOrderUpdate() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+updateVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+updateVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "      TestUtil.assertTrueTest(\n" +
        "          \""+ updateContents.get("A_TIMSTAMP")+"\",\n" +
        "          "+replCamel+".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
        "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
        "\n" +
        "      testUpdateData("+replCamel+".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addOutOfOrderDeleteTest() {
    lines.add("   /**\n" +
        "   * Event out of order with new record coming early.\n" +
        "   * For e.g. DELETE comes earlier than INSERT/UPDATE.\n" +
        "   */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Out-of-order: new DELETE record coming early\")\n" +
        "  public void testOutOfOrderDelete() {\n" +
        "    try {\n" +
        "\n" +
        "      "+serviceVariable+".processAsync("+deleteVariable+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue("+deleteVariable+", "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO \n"+
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "      TestUtil.assertTrueTest(\n" +
        "          \""+ deleteContents.get("A_TIMSTAMP")+"\",\n" +
        "          "+replCamel+".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
        "      assertEquals(true, "+replCamel+".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
        "\n" +
        "      testDeleteData("+replCamel+".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addIgnoreTest() {
    lines.add("  /**\n" +
        "   * Event out of order with older record coming later.\n" +
        "   * For e.g. INSERT comes later than UPDATE\n" +
        "   */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Out-of-order IGNORE: Older record coming later\")\n" +
        "  public void testIgnore() {\n" +
        "    try {\n" +
        "\n" +
        "      " + serviceVariable + ".processAsync(" + updateVariable + ");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      " + serviceVariable + ".processAsync(" + insertVariable + ");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      " + eventHubClassName + " " + eventHubClassName.toLowerCase() +
        " = mapper.readValue(" + updateVariable + ", " + eventHubClassName + ".class);\n" +
        "\n" +
        "      String uuid = #TODO \n" +
        "\n" +
        "      Optional<" + replicatedClassName + "> " + replCamel + " =\n" +
        "          " + repoVariable + ".findBy" + uuidColumn + "(uuid);\n" +
        "      assertNotNull(" + replCamel + ", \"" + replCamel + " is null\");\n" +
        "      assertTrue(" + replCamel + ".isPresent(), \"No " + replCamel + " present\");\n" +
        "      assertEquals(uuid, " + replCamel + ".get().get" + uuidColumn + "(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + " + replCamel + ".get().get" + uuidColumn + "());\n" +
        "      TestUtil.assertTrueTest(\n" +
        "          \""+ updateContents.get("A_TIMSTAMP")+"\",\n" +
        "          " + replCamel + ".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"PT\", " + replCamel + ".get().getDmlFlg());\n" +
        "      assertEquals(false, " + replCamel + ".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, " + replCamel + ".get().getDeletedIndicator());\n" +
        "      assertEquals(1,\n" +
        "          TestUtil.getLogMessageCountWithMessages(\n" +
        "              logCaptor.getInfoLogs(),\n" +
        "              \"Current item is earlier than DB entry.\"));\n" +
        "\n" +
        "      testUpdateData(" + replCamel + ".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addExceptionHandlingTest() {
    lines.add("  /** Test Exception Handling. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Exception Catch with Payload\")\n" +
        "  public void testProcessingExceptionHandlerPayload() {\n" +
        "    try {\n" +
        "      "+serviceVariable+".processAsync("+insertException+");\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "      verify(processingExceptionHandler, times(1)).handleUncaughtException(Mockito.any(), Mockito.any(), Mockito.any());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n");
  }

  public void addGenerateException() {
    lines.add("\n  /**\n" +
        "   * Generates ProcessingException with given payload and additional details.\n" +
        "   *\n" +
        "   * @param payload event as string\n" +
        "   * @return ProcessingException with details\n" +
        "   */\n" +
        "  public ProcessingException generateProcessingException(String payload) {\n" +
        "    ProcessingException processingException = new ProcessingException();\n" +
        "    processingException.setPayload(payload);\n" +
        "    processingException.setCurator("+serviceFileGenerator.serviceClassMapper+");\n" +
        "    processingException.setEventHubSource("+serviceFileGenerator.eventHubClassMapper+");\n" +
        "    processingException.setCuratedTarget("+serviceFileGenerator.replicatedClassMapper+");\n" +
        "    processingException.setRetryCount(1);\n" +
        "    return processingException;\n" +
        "  }\n");
  }

  public void addConsumeProcessingExceptionTest() {
    lines.add("\n  /** Test Consume ProcessingException. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Consume ProcessingException\")\n" +
        "  public void testConsumeProcessingException() {\n" +
        "    try {\n" +
        "      ProcessingException processingException = generateProcessingException("+insertVariable+");\n" +
        "\n" +
        "      "+serviceVariable+".processAsync(processingException);\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      "+eventHubClassName+" "+eventHubClassName.toLowerCase()+" = mapper.readValue(processingException.getPayload(), "+eventHubClassName+".class);\n" +
        "\n" +
        "      String uuid = #TODO\n" +
        "\n" +
        "      Optional<"+replicatedClassName+"> "+replCamel+" =\n" +
        "          "+repoVariable+".findBy"+uuidColumn+"(uuid);\n" +
        "      assertNotNull("+replCamel+", \""+replCamel+" is null\");\n" +
        "      assertTrue("+replCamel+".isPresent(), \"No "+replCamel+" present\");\n" +
        "      assertEquals(uuid, "+replCamel+".get().get"+ uuidColumn +"(),\n" +
        "          \"UUID: Expected=\" + uuid\n" +
        "              + \"; Actual=\" + "+replCamel+".get().get"+uuidColumn+"());\n" +
        "      TestUtil.assertTrueTest(\n" +
        "          \""+ insertContents.get("A_TIMSTAMP")+"\",\n" +
        "          "+replCamel+".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
        "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
        "\n" +
        "      testInsertData("+replCamel+".get());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n");
  }

  public void addExceptionHandlingProcessingExceptionTest() {
    lines.add("  /** Test Exception Handling. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Exception Catch with ProcessingException\")\n" +
        "  public void testProcessingExceptionHandlerProcessingException() {\n" +
        "    try {\n" +
        "      ProcessingException processingException = generateProcessingException("+insertVariable+"Exception);\n" +
        "\n" +
        "      "+serviceVariable+".processAsync(processingException);\n" +
        "      lock.await(1000, TimeUnit.MILLISECONDS);\n" +
        "\n" +
        "      verify(processingExceptionHandler, times(1)).handleUncaughtException(Mockito.any(), Mockito.any(), Mockito.any());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n");
  }

  public void addTestDataFields(String op) {
    String pk = replicatedFileGenerator.ddlsqlFileGenerator.uuidColumnNames.get(0);
    for (Map.Entry<String, String> entry: replicatedFileGenerator.columnTypes.entrySet()) {
      String field = entry.getKey();
      String value = entry.getValue();
      String fieldUp = StringUtils.capitalize(FileUtil.getFieldName(field));
      if (pk.equals(field) || replicatedFileGenerator.ehBaseColumnsSet.contains(field) || field.startsWith("B_")) {
        continue;
      }
      String fieldValue;
      switch (op) {
        case "PT" -> fieldValue = insertContents.get(field)==null? null:"\"" + insertContents.get(field) + "\"";
        case "UP" -> fieldValue = updateContents.get(field)==null? null:"\"" + updateContents.get(field) + "\"";
        case "DL" -> fieldValue = deleteContents.get("B_"+field)==null? null:"\"" + deleteContents.get("B_"+field) + "\"";
        default -> throw new IllegalArgumentException(op + "not supported");
      }
      if (field.equalsIgnoreCase("TICKET_CREATE_TS")) {
        lines.add("    assertEquals("+fieldValue+", target.getTicketCreateTs"+"().toString());\n");
        continue;
      }
      if (replicatedFileGenerator.ddlsqlFileGenerator.uuidColumnNames.contains(field)) {
        lines.add("    assertEquals(, target.get"+ fieldUp+"());\n");
        continue;
      }
      if (value.equalsIgnoreCase("timestamp")) {
        if(fieldValue!=null) {
          fieldValue = fieldValue.substring(0, 24).replace('T', ' ')+"\"";
        }
        if (fieldValue != null) {
          lines.add("    TestUtil.assertTimestamps("+fieldValue+", target.get"+fieldUp+"(), formatter);\n");
        } else {
          lines.add("    assertEquals("+fieldValue+", target.get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
        }

      } else if (value.equalsIgnoreCase("date")) {
        lines.add("    TestUtil.assertTrueTest("+fieldValue+", target.get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
      } else if(!value.equalsIgnoreCase("string")) {
        if (fieldValue !=null ) {
          lines.add("    assertEquals("+fieldValue+", target.get"+fieldUp+"().toString(), \""+ fieldUp+" are not equal.\");\n");
        } else {
          lines.add("    assertEquals("+fieldValue+", target.get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
        }
      } else {
        lines.add("    assertEquals("+fieldValue+", target.get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
      }
    }
  }

  public void addMethods() throws IOException {
    addFields();
    lines.add("\n\n  @BeforeEach\n" +
        "  public void removeDbEntries() {\n" +
        "    "+repoVariable+".deleteAll();\n" +
        "  }\n\n");

    lines.add("  @BeforeEach\n" +
        "  public void setLogCaptor() {\n" +
        "    logCaptor = LogCaptor.forClass("+this.serviceClassName+".class);\n" +
        "  }\n\n");

    //#TODO
    lines.add("  /**\n" +
        "   * Tests all the columns from Insert event.\n" +
        "   *\n" +
        "   * @param target "+replicatedClassName+" object\n" +
        "   */\n" +
        "  public void testInsertData("+replicatedClassName+" target) { \n");
    addTestDataFields("PT");
    lines.add("  }\n\n");
    lines.add("  /**\n" +
        "   * Tests all the columns from Update event.\n" +
        "   *\n" +
        "   * @param target "+replicatedClassName+" object\n" +
        "   */\n" +
        "  public void testUpdateData("+replicatedClassName+" target) { \n");
    addTestDataFields("UP");
    lines.add("  }\n\n");
    lines.add("  /**\n" +
        "   * Tests all the columns from Delete event.\n" +
        "   *\n" +
        "   * @param target "+replicatedClassName+" object\n" +
        "   */\n" +
        "  public void testDeleteData("+replicatedClassName+" target) {\n");
    addTestDataFields("DL");
    lines.add("  }\n\n");
    addInsertTest();
    addUpdateTest();
    addConcurrentInsertTest();
    addConcurrentUpdateTest();
    addDeleteTest();
    addOutOfOrderUpdateTest();
    addOutOfOrderDeleteTest();
    addIgnoreTest();
    addExceptionHandlingTest();
    addGenerateException();
    addConsumeProcessingExceptionTest();
    addExceptionHandlingProcessingExceptionTest();
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
    addMethods();
    addEndingLine();
    this.generatedOutput = String.join("", lines);
//    System.out.println(this.generatedOutput);
    try {
      writer.write(this.generatedOutput);
    } finally {
      writer.close();
    }
    System.out.println("Test file successfully generated. Please review at location: " + fullPath);
    System.out.println("\tNOTE: Please review the generated code.");
  }
}
