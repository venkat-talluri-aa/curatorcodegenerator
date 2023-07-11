package com.aa.rac.mod;

import com.aa.rac.mod.codegenerator.ConverterFileGenerator;
import com.aa.rac.mod.codegenerator.DDLSQLFileGenerator;
import com.aa.rac.mod.codegenerator.FileUtil;
import com.aa.rac.mod.codegenerator.EventHubPojoGenerator;
import com.aa.rac.mod.codegenerator.ReplicatedFileGenerator;
import com.aa.rac.mod.codegenerator.RepositoryFileGenerator;
import com.aa.rac.mod.codegenerator.ServiceFileGenerator;
import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.convert.converter.Converter;

@SpringBootApplication
public class CuratorcodegeneratorApplication {
	public static final String SCHEMA_NAME = "curated_test";

	public static void main(String[] args) throws IOException {
		SpringApplication.run(CuratorcodegeneratorApplication.class, args);
		String resourcesPath = System.getProperty("user.dir").replace('\\', '/')
				+ "/src/main/resources/";

		String fileName = "agdename";

		String filePath = resourcesPath + fileName + ".txt";
		String[] uuids = {"NAME_UUID"};

		String ddlFilePath = resourcesPath + fileName + "_ddl.txt";
		DDLSQLFileGenerator ddlsqlFileGenerator = new DDLSQLFileGenerator(ddlFilePath, FileUtil.getClassName(filePath).toLowerCase(), uuids);
		ddlsqlFileGenerator.generateDDLFile();
		System.out.println("\n");

		EventHubPojoGenerator eventHubPojoGenerator = new EventHubPojoGenerator(filePath, ddlsqlFileGenerator);
		eventHubPojoGenerator.eventHubPojoFileGenerator();
		System.out.println("\n");

		ReplicatedFileGenerator replicatedFileGenerator = new ReplicatedFileGenerator(filePath, ddlsqlFileGenerator);
		replicatedFileGenerator.generateReplicatedFile();
		System.out.println("\n");

		String replImportPath = replicatedFileGenerator.getReplicatedImportPath();
		String replClassName = replImportPath.substring(replImportPath.lastIndexOf(".")+1);

		RepositoryFileGenerator repositoryFileGenerator = new RepositoryFileGenerator(ddlsqlFileGenerator, replClassName);
		repositoryFileGenerator.generateRepositoryFile(replImportPath);


		System.out.println("\n");
		ConverterFileGenerator converterFileGenerator = new ConverterFileGenerator(replicatedFileGenerator, eventHubPojoGenerator, ddlsqlFileGenerator);
		converterFileGenerator.generateConverterFile();

		System.out.println("\n");
		ServiceFileGenerator serviceFileGenerator = new ServiceFileGenerator(replicatedFileGenerator, eventHubPojoGenerator, repositoryFileGenerator);
		serviceFileGenerator.generateConverterFile();
	}

}
