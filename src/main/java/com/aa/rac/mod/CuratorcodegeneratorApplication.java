package com.aa.rac.mod;

import com.aa.rac.mod.codegenerator.ConverterFileGenerator;
import com.aa.rac.mod.codegenerator.DDLSQLFileGenerator;
import com.aa.rac.mod.codegenerator.FileUtil;
import com.aa.rac.mod.codegenerator.EventHubPojoGenerator;
import com.aa.rac.mod.codegenerator.ReplicatedFileGenerator;
import com.aa.rac.mod.codegenerator.RepositoryFileGenerator;
import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CuratorcodegeneratorApplication {
	public static final String SCHEMA_NAME = "curated_test";

	public static void main(String[] args) throws IOException {
		SpringApplication.run(CuratorcodegeneratorApplication.class, args);
		String resourcesPath = System.getProperty("user.dir").replace('\\', '/')
				+ "/src/main/resources/";
		String filePath = resourcesPath + "refunded.txt";
		String uuid = "refunded_uuid";
		EventHubPojoGenerator eventHubPojoGenerator = new EventHubPojoGenerator(filePath);
		eventHubPojoGenerator.eventHubPojoFileGenerator();
		System.out.println("\n\n");


		ReplicatedFileGenerator replicatedFileGenerator = new ReplicatedFileGenerator(filePath);
		replicatedFileGenerator.generateReplicatedFile(uuid);
		System.out.println("\n\n");

		String replImportPath = replicatedFileGenerator.getReplicatedImportPath();
		String replClassName = replImportPath.substring(replImportPath.lastIndexOf(".")+1);

		RepositoryFileGenerator repositoryFileGenerator = new RepositoryFileGenerator(replClassName);
		repositoryFileGenerator.generateRepositoryFile(replImportPath, uuid);

		String replFilePath = "C:/Users/vtalluri/OneDrive - Insight/Documents/1 - AA/RAC MOD Project/Java/1-AAInternal/racmodcurator-pb/src/main/java/com/aa/rac/mod/orm/dao/refundedfa/RefundedFa.java";
		ConverterFileGenerator converterFileGenerator = new ConverterFileGenerator(replicatedFileGenerator, eventHubPojoGenerator, replFilePath);
		converterFileGenerator.generateConverterFile();

		String ddlFilePath = resourcesPath + "refunded_ddl.txt";
		DDLSQLFileGenerator ddlsqlFileGenerator = new DDLSQLFileGenerator(ddlFilePath, FileUtil.getClassName(filePath).toLowerCase());
		ddlsqlFileGenerator.generateDDLFile(uuid);
	}

}
