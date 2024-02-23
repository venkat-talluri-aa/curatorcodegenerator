package com.aa.rac.mod.codegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.util.StringUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdhoDeleteServiceTestFileGenerator {

  private static final String packageName = "com.aa.rac.mod.service.adhocdeletes.agmmtkts.";

  private String adhocDeleteEventhubClassName = "AgmmtktsAdhocDelete";
  private String adhocDeleteServiceVar = "adhocDeleteService";
  private String eventHubClassName;

  private String replicatedClassName;

  private String repositoryClassName;

  private String serviceClassName;
  private String otherServiceClassName;

  private String mockedServiceClassName;

  private String spyServiceClassName;

  private String testClassName;

  private static final String end = ";";
  private FileWriter fileWriter = null;

  private List<String> lines = new ArrayList<>();

  private AdhocDeleteServiceFileGenerator serviceFileGenerator;

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

  private String hexCharsVariable;

  private String logPiiException;

  String insFilepath;
  String updFilepath;
  String delFilepath;

  public Map<String, String> insertContents = new HashMap<>();
  public Map<String, String> updateContents = new HashMap<>();
  public Map<String, String> deleteContents = new HashMap<>();

  public AdhoDeleteServiceTestFileGenerator(AdhocDeleteServiceFileGenerator serviceFileGenerator, DDLSQLFileGenerator ddlsqlFileGenerator, String insFilepath, String updFilepath, String delFilepath)
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
    this.serviceClassName = this.eventHubClassName + "AdhocDeleteServiceImpl";
    this.otherServiceClassName = this.replicatedClassName + "ServiceImpl";
    this.testClassName = this.serviceClassName + "Test";
    this.serviceVariable = this.eventHubClassName.toLowerCase() + "Service";
    this.repoVariable = this.repositoryClassName.substring(0,1).toLowerCase()
        + this.repositoryClassName.substring(1);
    this.insertVariable = "insert"+this.eventHubClassName;
    this.updateVariable = "update"+this.eventHubClassName;
    this.deleteVariable = "delete"+this.eventHubClassName;
    this.hexCharsVariable = "hexChars"+this.eventHubClassName;
    this.logPiiException = "logPii"+this.eventHubClassName+"Exception";
    this.replCamel = this.replicatedClassName.substring(0, 1).toLowerCase() + this.replicatedClassName.substring(1);
    this.uuidColumn = StringUtils.capitalize(FileUtil.getFieldName(repositoryFileGenerator.uuidColumnName));
    this.insertException = this.insertVariable +"Exception";
    this.mockedServiceClassName = this.serviceClassName.substring(0, 1).toLowerCase()+this.serviceClassName.substring(1).replace("Impl", "");
    this.spyServiceClassName = this.mockedServiceClassName + "Spy";
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
    return serviceFileGenerator.getServiceDirectory().replace("main", "test");
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
            "import static org.junit.jupiter.api.Assertions.assertFalse;\n" +
        "import static org.junit.jupiter.api.Assertions.assertNotNull;\n" +
        "import static org.junit.jupiter.api.Assertions.assertNull;\n" +
        "import static org.junit.jupiter.api.Assertions.assertThrows;\n" +
        "import static org.junit.jupiter.api.Assertions.assertTrue;\n" +
        "import static org.junit.jupiter.api.Assertions.fail;\n" +
        "import static org.mockito.ArgumentMatchers.any;\n" +
        "import static org.mockito.ArgumentMatchers.anyInt;\n" +
        "import static org.mockito.ArgumentMatchers.anyString;\n" +
        "import static org.mockito.Mockito.doThrow;\n" +
        "import static org.mockito.Mockito.spy;\n" +
        "import static org.mockito.Mockito.times;\n" +
        "import static org.mockito.Mockito.verify;\n" +
        "\n" +
        "import com.aa.rac.mod.domain.enums.ExceptionType;\n" +
        "import com.aa.rac.mod.domain.exceptions.ProcessingException;\n" +
        "import com.aa.rac.mod.domain.exceptions.ProcessingExceptionHandler;\n" +
        "import com.aa.rac.mod.domain.exceptions.QueueException;\n" +
        "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import com.aa.rac.mod.repository.eventhub.AgmmtktsAdhocDelete;\n" +
        "import " + repositoryImportPath + ";\n" +
        "import com.aa.rac.mod.service.BaseService;\n" +
        "import com.aa.rac.mod.util.AbstractTestSupport;\n" +
        "import com.aa.rac.mod.util.TestUtil;\n" +
        "import com.azure.messaging.servicebus.ServiceBusSenderClient;\n" +
        "import com.fasterxml.jackson.databind.ObjectMapper;\n" +
        "import java.math.BigInteger;\n" +
        "import java.time.format.DateTimeFormatter;\n" +
        "import java.util.Optional;\n" +
        "import java.util.concurrent.CountDownLatch;\n" +
            "import java.util.concurrent.ExecutionException;\n" +
            "import java.util.concurrent.Future;\n" +
        "import java.util.concurrent.TimeUnit;\n" +
        "import org.junit.jupiter.api.BeforeEach;\n" +
        "import org.junit.jupiter.api.DisplayName;\n" +
        "import org.junit.jupiter.api.Test;\n" +
        "import org.junit.jupiter.api.extension.ExtendWith;\n" +
            "import org.mockito.ArgumentCaptor;\n" +
        "import org.mockito.Captor;\n" +
        "import org.mockito.InjectMocks;\n" +
        "import org.mockito.junit.jupiter.MockitoExtension;\n" +
        "import org.springframework.beans.factory.annotation.Autowired;\n" +
            "import org.springframework.beans.factory.annotation.Qualifier;\n" +
        "import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;\n" +
        "import org.springframework.boot.test.context.SpringBootTest;\n" +
        "import org.springframework.boot.test.mock.mockito.MockBean;\n" +
            "import org.springframework.boot.test.system.CapturedOutput;\n" +
            "import org.springframework.boot.test.system.OutputCaptureExtension;\n" +
            "import org.springframework.data.mapping.MappingException;\n" +
            "import org.springframework.kafka.annotation.EnableKafka;\n" +
            "import org.springframework.kafka.core.KafkaTemplate;\n" +
            "import org.springframework.kafka.test.context.EmbeddedKafka;\n";
    lines.add(imports +"\n\n");
  }

  public void addClassJavaDoc() {
    lines.add("/** " + serviceClassName + " H2 test. */\n");
  }

  public String getClassAnnotations() {
    return "@EnableKafka\n" +
            "@SpringBootTest\n" +
            "@EmbeddedKafka(\n" +
            "    partitions = 1,\n" +
            "    controlledShutdown = false,\n" +
            "    brokerProperties = {\"listeners=PLAINTEXT://localhost:3333\", \"port=3333\"})\n" +
        "@AutoConfigureTestDatabase\n" +
        "@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})\n" +
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
        "  @MockBean KafkaTemplate<String, String> kafkaTemplate;\n" +
        "\n" +
        "  @Captor ArgumentCaptor<String> throwableCaptor;\n" +
        "\n" +
        "  @MockBean ProcessingExceptionHandler processingExceptionHandler;\n" +
            "\n" +
        "  @MockBean ServiceBusSenderClient serviceBusSenderClient;\n");
    lines.add("\n  @Autowired \n" +
        "  private " + repositoryClassName + " "
        + repoVariable + ";");
    lines.add("\n\n  @Autowired\n  @Qualifier(\""+serviceClassName+"\")\n  private BaseService<"+adhocDeleteEventhubClassName+", "+replicatedClassName+"> "+adhocDeleteServiceVar+";");
    lines.add("\n\n  @Autowired\n  @Qualifier(\""+otherServiceClassName+"\")\n  private BaseService<"+eventHubClassName+", "+replicatedClassName+"> " + serviceVariable+";");
    lines.add("\n\n  @InjectMocks\n  " + serviceClassName + " " + mockedServiceClassName+";");
    lines.add("\n\n  " + serviceClassName + " " + spyServiceClassName + ";");
  }

  public void addEndingFields() throws IOException {
    lines.add("\n  private final String " + insertVariable + " = \""
            + String.join("", FileUtil.readLinesFromFile(insFilepath)).replace("\"", "\\\"")+"\";");
    lines.add("\n\n  private final String oldDeleteAdhoc = #TODO;");
    lines.add("\n\n  private final String deleteAdhoc = #TODO;");
    lines.add("\n\n  private final String deleteAdhocException = #TODO;");
    lines.add("\n\n  private final String notDeleteAdhoc = #TODO;");
    lines.add("\n\n  private final String unrelatedDeleteAdhoc = #TODO;");
    lines.add("\n\n  private final String deleteAgmmtktsAdhocException = #TODO;");
    lines.add("\n\n  private final String deleteAgmmtktsAdhoc = #TODO;\n\n");
  }
  public void addAdhocDeleteWithPartialAgmmtktsPayloadTest() {
    lines.add("\n  /** Test Adhoc Delete with partial AGMMTKTS payload. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"ADHOC DELETE with partial payload\")\n" +
            "  public void testAdhocDeleteWithPartialAgmmtktsPayload() {\n" +
            "    try {\n" +
            "\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      "+adhocDeleteServiceVar+".processAsync(deleteAdhoc).get();\n" +
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
            "      TestUtil.assertTrueTest(\n          ,\n" +
            "          "+replCamel+".get().getEventHubTimestamp(),\n" +
            "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
            "          \"EventHubTimestamp are not equal\");\n" +
            "\n" +
            "      assertEquals(\"UP\", "+replCamel+".get().getDmlFlg());\n" +
            "      assertEquals(true, "+replCamel+".get().getSrcDeletedIndicator());\n" +
            "      assertEquals(true, "+replCamel+".get().getDeletedIndicator());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getCreatedBy());\n" +
            "      assertEquals(\"JAVA_"+serviceClassName+"\", "+replCamel+".get().getModifiedBy());\n" +
            "\n" +
            "      testInsertData("+replCamel+".get());\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addAdhocDeleteWithFullAgmmtktsPayloadTest() {
    lines.add("\n  /** Test Adhoc Delete with full AGMMTKTS payload. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"ADHOC DELETE with full payload\")\n" +
            "  public void testAdhocDeleteWithFullAgmmtktsPayload() {\n" +
            "    try {\n" +
            "\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      "+adhocDeleteServiceVar+".processAsync(deleteAgmmtktsAdhoc).get();\n" +
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
            "      TestUtil.assertTrueTest(\n          ,\n" +
            "          "+replCamel+".get().getEventHubTimestamp(),\n" +
            "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
            "          \"EventHubTimestamp are not equal\");\n" +
            "\n" +
            "      assertEquals(\"UP\", "+replCamel+".get().getDmlFlg());\n" +
            "      assertEquals(true, "+replCamel+".get().getSrcDeletedIndicator());\n" +
            "      assertEquals(true, "+replCamel+".get().getDeletedIndicator());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getCreatedBy());\n" +
            "      assertEquals(\"JAVA_"+serviceClassName+"\", "+replCamel+".get().getModifiedBy());\n" +
            "\n" +
            "      testInsertData("+replCamel+".get());\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addAdhocDeleteOldTest() {
    lines.add("\n  /** Test Old Adhoc Delete. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"Old ADHOC DELETE\")\n" +
            "  public void testAdhocDeleteOld() {\n" +
            "    try {\n" +
            "\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      "+adhocDeleteServiceVar+".processAsync(oldDeleteAdhoc).get();\n" +
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
            "          \""+ insertContents.get("A_TIMSTAMP")+"\",\n" +
            "          "+replCamel+".get().getEventHubTimestamp(),\n" +
            "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
            "          \"EventHubTimestamp are not equal\");\n" +
            "\n" +
            "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
            "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
            "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getCreatedBy());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getModifiedBy());\n" +
            "\n" +
            "      testInsertData("+replCamel+".get());\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addNotAdhocDeleteTest() {
    lines.add("\n  /** Test sending not Adhoc Delete. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"NOT ADHOC DELETE\")\n" +
            "  public void testNotAdhocDelete() {\n" +
            "    try {\n" +
            "\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      "+adhocDeleteServiceVar+".processAsync(notDeleteAdhoc).get();\n" +
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
            "          \""+ insertContents.get("A_TIMSTAMP")+"\",\n" +
            "          "+replCamel+".get().getEventHubTimestamp(),\n" +
            "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
            "          \"EventHubTimestamp are not equal\");\n" +
            "\n" +
            "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
            "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
            "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getCreatedBy());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getModifiedBy());\n" +
            "\n" +
            "      testInsertData("+replCamel+".get());\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addUnrelatedAdhocDeleteTest() {
    lines.add("\n  /** Test sending unrelated Adhoc Delete. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"Unrelated ADHOC DELETE\")\n" +
            "  public void testUnrelatedAdhocDelete() {\n" +
            "    try {\n" +
            "\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      "+adhocDeleteServiceVar+".processAsync(unrelatedDeleteAdhoc).get();\n" +
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
            "          \""+ insertContents.get("A_TIMSTAMP")+"\",\n" +
            "          "+replCamel+".get().getEventHubTimestamp(),\n" +
            "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
            "          \"EventHubTimestamp are not equal\");\n" +
            "\n" +
            "      assertEquals(\"PT\", "+replCamel+".get().getDmlFlg());\n" +
            "      assertEquals(false, "+replCamel+".get().getSrcDeletedIndicator());\n" +
            "      assertEquals(false, "+replCamel+".get().getDeletedIndicator());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getCreatedBy());\n" +
            "      assertEquals(\"JAVA_"+otherServiceClassName+"\", "+replCamel+".get().getModifiedBy());\n" +
            "\n" +
            "      testInsertData("+replCamel+".get());\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addProcessingExceptionHandlerPayloadTest() {
    lines.add("\n  /** Test Exception Handling. */\n" +
        "  @Test\n" +
        "  @DisplayName(\"Exception Catch with Payload\")\n" +
        "  public void testProcessingExceptionHandlerPayload() {\n" +
        "    try {\n" +
        "      "+adhocDeleteServiceVar+".processAsync(deleteAdhocException).get();\n" +
        "      verify(processingExceptionHandler, times(1))\n" +
            "          .submitToExceptionQueue(throwableCaptor.capture(), any(), anyInt());\n" +
            "      String payload = throwableCaptor.getValue();\n" +
            "      "+adhocDeleteEventhubClassName + " " + adhocDeleteEventhubClassName.replace("AdhocDelete", "").toLowerCase()
            + " = mapper.readValue(deleteAdhocException, "+adhocDeleteEventhubClassName+ ".class);\n" +
            "      "+adhocDeleteEventhubClassName+" exception"+adhocDeleteEventhubClassName.replace("AdhocDelete", "").toLowerCase()+
            " = mapper.readValue(payload, "+adhocDeleteEventhubClassName+".class);\n" +
            "      #TODO: Test some columns using assertions\n" +
            "      assertEquals("+eventHubClassName.toLowerCase()+".get, exceptionAgdename.get);\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n\n");
  }

  public void addQueueExceptionTest() {
    lines.add("  /** Test Logging Masked Payload with QueueException. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"Logging Masked Payload with QueueException\")\n" +
            "  public void testLoggingMaskedPayloadOnQueueException(CapturedOutput output) {\n" +
            "    try {\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      doThrow(QueueException.class)\n" +
            "              .when(processingExceptionHandler)\n" +
            "              .submitToExceptionQueue(anyString(), any(ProcessingException.class), anyInt());\n" +
            "\n" +
            "      assertThrows(ExecutionException.class, () -> {\n" +
            "        "+adhocDeleteServiceVar+".processAsync(deleteAgmmtktsAdhocException).get();\n" +
            "      });\n" +
            "      verify(processingExceptionHandler, times(1))\n" +
            "              .submitToExceptionQueue(anyString(), any(ProcessingException.class), anyInt());\n" +
            "      assertTrue(TestUtil.checkLogMessageContains(output.getErr(),\n" +
            "              \"Throwing QueueException for payload\"));\n" +
            "      #TODO: Test masked columns from paylod using assertions\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addMappingExceptionTest() {
    lines.add ("  /** Test Logging Masked Payload with MappingException. */\n" +
            "  @Test\n" +
            "  @DisplayName(\"Send to DLQ with MappingException\")\n" +
            "  public void testMappingException(CapturedOutput output) {\n" +
            "    try {\n" +
            "      "+serviceVariable+".processAsync("+insertVariable+").get();\n" +
            "\n" +
            "      doThrow(MappingException.class)\n" +
            "          .when("+spyServiceClassName+")\n" +
            "          .mapExceptionDetailsToEventhubObject(any(), any());\n" +
            "\n" +
            "      "+spyServiceClassName+".processAsync(deleteAgmmtktsAdhocException).get();\n" +
            "      verify("+spyServiceClassName+", times(1)).mapExceptionDetailsToEventhubObject(any(), any());\n" +
            "      assertTrue(\n" +
            "          TestUtil.checkLogMessageContains(\n" +
            "              output.getErr(), \"Encountered JsonProcessingException/MappingException for payload\"));\n" +
            "      verify(processingExceptionHandler, times(1)).sendWithDeadLetterSubject(anyString());\n" +
            "    } catch (Exception e) {\n" +
            "      fail(e.getMessage(), e);\n" +
            "    }\n" +
            "  }\n\n");
  }

  public void addTestDataFieldsTarget(String op, String objectName) {
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
        lines.add("    assertEquals("+fieldValue+", "+objectName+".getTicketCreateTs"+"().toString());\n");
        continue;
      }
      if (replicatedFileGenerator.ddlsqlFileGenerator.uuidColumnNames.contains(field)) {
        lines.add("    assertEquals(, "+objectName+".get"+ fieldUp+"());\n");
        continue;
      }
      if (value.equalsIgnoreCase("timestamp")) {
        if(fieldValue!=null) {
          fieldValue = fieldValue.substring(0, 24).replace('T', ' ')+"\"";
        }
        if (fieldValue != null) {
          lines.add("    TestUtil.assertTimestamps("+fieldValue+", "+objectName+".get"+fieldUp+"(), formatter);\n");
        } else {
          lines.add("    assertEquals("+fieldValue+", "+objectName+".get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
        }

      } else if (value.equalsIgnoreCase("date")) {
        lines.add("    TestUtil.assertTrueTest("+fieldValue+", "+objectName+".get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
      } else if(!value.equalsIgnoreCase("string")) {
        if (fieldValue !=null ) {
          lines.add("    assertEquals("+fieldValue+", "+objectName+".get"+fieldUp+"().toString(), \""+ fieldUp+" are not equal.\");\n");
        } else {
          lines.add("    assertEquals("+fieldValue+", "+objectName+".get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
        }
      } else {
        lines.add("    assertEquals("+fieldValue+", "+objectName+".get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
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
        "  public void setServiceSpy() {\n" +
        "    " + spyServiceClassName+" = spy("+ mockedServiceClassName+");\n" +
        "  }\n\n");
    addAdhocDeleteWithPartialAgmmtktsPayloadTest();
    addAdhocDeleteWithFullAgmmtktsPayloadTest();
    addAdhocDeleteOldTest();
    addNotAdhocDeleteTest();
    addUnrelatedAdhocDeleteTest();
    addProcessingExceptionHandlerPayloadTest();
    addQueueExceptionTest();
    addMappingExceptionTest();
    lines.add("  /**\n" +
            "   * Tests all the columns from Insert event.\n" +
            "   *\n" +
            "   * @param target "+replicatedClassName+" object\n" +
            "   */\n" +
            "  public void testInsertData("+replicatedClassName+" target) { \n");
    addTestDataFieldsTarget("PT", "target");
    lines.add("  }\n\n");
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
    addEndingFields();
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
