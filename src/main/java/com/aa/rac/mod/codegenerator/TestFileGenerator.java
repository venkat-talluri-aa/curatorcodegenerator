package com.aa.rac.mod.codegenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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

  private ReplicatedFileGenerator replicatedFileGenerator;

  private EventHubPojoGenerator eventHubPojoGenerator;

  private RepositoryFileGenerator repositoryFileGenerator;

  private String generatedOutput;

  private String serviceVariable;
  private String repoVariable;

  private String insertVariable;
  private String updateVariable;
  private String deleteVariable;
  private String replCamel;

  private String uuidColumn;

  private String insertException;

  public TestFileGenerator(ReplicatedFileGenerator replicatedFileGenerator,
                           EventHubPojoGenerator eventHubPojoGenerator,
                           RepositoryFileGenerator repositoryFileGenerator) {
    this.replicatedFileGenerator = replicatedFileGenerator;
    this.eventHubPojoGenerator = eventHubPojoGenerator;
    this.repositoryFileGenerator = repositoryFileGenerator;
    this.eventHubClassName = eventHubPojoGenerator.getEventHubImportPath().substring(eventHubPojoGenerator.getEventHubImportPath().lastIndexOf('.')+1);
    this.replicatedClassName = replicatedFileGenerator.getReplicatedImportPath().substring(replicatedFileGenerator.getReplicatedImportPath().lastIndexOf('.')+1);
    this.repositoryClassName = repositoryFileGenerator.getRepositoryImportPath().substring(repositoryFileGenerator.getRepositoryImportPath().lastIndexOf('.')+1);
    this.serviceClassName = this.replicatedClassName + "ServiceImpl";
    this.testClassName = this.serviceClassName + "H2Test";
    this.serviceVariable = this.eventHubClassName.toLowerCase() + "Service";
    this.repoVariable = this.repositoryClassName.substring(0,1).toLowerCase()
        + this.repositoryClassName.substring(1);
    this.insertVariable = "insert"+this.eventHubClassName;
    this.updateVariable = "update"+this.eventHubClassName;
    this.deleteVariable = "delete"+this.eventHubClassName;
    this.replCamel = this.replicatedClassName.substring(0, 1).toLowerCase() + this.replicatedClassName.substring(1);
    this.uuidColumn = StringUtils.capitalize(FileUtil.getFieldName(repositoryFileGenerator.uuidColumnName));
    this.insertException = this.insertVariable +"Exception";
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
        "import com.aa.rac.mod.domain.enums.ServiceClassMapper;\n" +
        "import com.aa.rac.mod.domain.exceptions.ProcessingExceptionHandler;\n" +
        "import com.aa.rac.mod.domain.util.RacUtil;\n" +
        "import com.aa.rac.mod.domain.util.TestUtil;\n" +
        "import " + replicatedImportPath + ";\n" +
        "import " + eventHubImportPath + ";\n" +
        "import " + repositoryImportPath + ";\n" +
        "import com.aa.rac.mod.service.ServiceFactory;\n"+
        "import com.fasterxml.jackson.databind.ObjectMapper;\n" +
        "import java.math.BigInteger;\n" +
        "import java.time.format.DateTimeFormatter;\n" +
        "import java.util.Optional;\n" +
        "import java.util.concurrent.CountDownLatch;\n" +
        "import java.util.concurrent.TimeUnit;\n" +
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
    lines.add("public class " + testClassName +" {\n");
  }

  public void addFields() {
    lines.add("\n  private final CountDownLatch lock = new CountDownLatch(1);\n" +
        "  private final ObjectMapper mapper = new ObjectMapper();\n" +
        "\n" +
        "  DateTimeFormatter formatter = DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSS\");\n" +
        "\n" +
        "  @MockBean\n" +
        "  ProcessingExceptionHandler processingExceptionHandler;\n");
    lines.add("\n  @Autowired \n" +
        "  private " + repositoryClassName + " "
        + repoVariable + ";");
    lines.add("\n\n  private BaseService " + serviceVariable+";");
    lines.add("\n\n\n  private final String " + insertVariable + " = #TODO");
    lines.add("\n\n  private final String " + updateVariable + " = #TODO");
    lines.add("\n\n  private final String " + deleteVariable + " = #TODO");
    lines.add("\n\n  private final String " + insertException + " = #TODO");
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
        "      TestUtil.assertTrueTest(\n" +
        "          ,\n" +
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
        "          ,\n" +
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
        "          ,\n" +
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
        "          ,\n" +
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
        "          ,\n" +
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
        "          ,\n" +
        "          " + replCamel + ".get().getEventHubTimestamp(),\n" +
        "          DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSSSSS\"),\n" +
        "          \"EventHubTimestamp are not equal\");\n" +
        "\n" +
        "      assertEquals(\"PT\", " + replCamel + ".get().getDmlFlg());\n" +
        "      assertEquals(false, " + replCamel + ".get().getSrcDeletedIndicator());\n" +
        "      assertEquals(false, " + replCamel + ".get().getDeletedIndicator());\n" +
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
        "  @DisplayName(\"Exception Catch\")\n" +
        "  public void testProcessingExceptionHandler() {\n" +
        "    try {\n" +
        "      "+serviceVariable+".processAsync("+insertException+");\n" +
        "      lock.await(2000, TimeUnit.MILLISECONDS);\n" +
        "      verify(processingExceptionHandler, times(1)).handleUncaughtException(Mockito.any(), Mockito.any(), Mockito.any());\n" +
        "    } catch (Exception e) {\n" +
        "      fail(e.getMessage(), e);\n" +
        "    }\n" +
        "  }\n");
  }

  public void addTestDataFields() {
    String pk = replicatedFileGenerator.ddlsqlFileGenerator.uuidColumnNames.get(0);
    for (Map.Entry<String, String> entry: replicatedFileGenerator.columnTypes.entrySet()) {
      String field = entry.getKey();
      String value = entry.getValue();
      String fieldUp = StringUtils.capitalize(FileUtil.getFieldName(field));
      if (pk.equals(field) || replicatedFileGenerator.ehBaseColumnsSet.contains(field) || field.startsWith("B_")) {
        continue;
      }
      if (field.equalsIgnoreCase("TICKET_CREATE_TS")) {
        lines.add("    assertEquals(, target.getTicketCreateTs"+"().toString());\n");
        continue;
      }
      if (replicatedFileGenerator.ddlsqlFileGenerator.uuidColumnNames.contains(field)) {
        lines.add("    assertEquals(, target.get"+ fieldUp+"());\n");
        continue;
      }
      if (value.equalsIgnoreCase("timestamp")) {
        lines.add("    TestUtil.assertTimestamps(, target.get"+fieldUp+"(), formatter);\n");
      } else if (value.equalsIgnoreCase("date")) {
        lines.add("    TestUtil.assertTrueTest(, target.get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
      } else {
        lines.add("    assertEquals(, target.get"+fieldUp+"(), \""+ fieldUp+" are not equal.\");\n");
      }
    }
  }

  public void addMethods() {
    addFields();
    lines.add("\n\n /** Set Base services by generating with ServiceFactory. */\n" +
        " @BeforeEach\n" +
        "  public void setBaseService() {\n" +
        "    "+ serviceVariable +" = ServiceFactory.getBaseService(\n" +
        "        ServiceClassMapper."+replicatedClassName.toUpperCase()+"_SERVICE_IMPL);\n" +
        "  }\n\n");
    lines.add("  @BeforeEach\n" +
        "  public void removeDbEntries() {\n" +
        "    "+repoVariable+".deleteAll();\n" +
        "  }\n\n");

    //#TODO
    lines.add("  /**\n" +
        "   * Tests all the columns from Insert event.\n" +
        "   *\n" +
        "   * @param target "+replicatedClassName+" object\n" +
        "   */\n" +
        "  public void testInsertData("+replicatedClassName+" target) { \n");
    addTestDataFields();
    lines.add("  }\n\n");
    lines.add("  /**\n" +
        "   * Tests all the columns from Update event.\n" +
        "   *\n" +
        "   * @param target "+replicatedClassName+" object\n" +
        "   */\n" +
        "  public void testUpdateData("+replicatedClassName+" target) { \n");
    addTestDataFields();
    lines.add("  }\n\n");
    lines.add("  /**\n" +
        "   * Tests all the columns from Delete event.\n" +
        "   *\n" +
        "   * @param target "+replicatedClassName+" object\n" +
        "   */\n" +
        "  public void testDeleteData("+replicatedClassName+" target) {\n");
    addTestDataFields();
    lines.add("  }\n\n");
    addInsertTest();
    addUpdateTest();
    addDeleteTest();
    addOutOfOrderUpdateTest();
    addOutOfOrderDeleteTest();
    addIgnoreTest();
    addExceptionHandlingTest();
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
