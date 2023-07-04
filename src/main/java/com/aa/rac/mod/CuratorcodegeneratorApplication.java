package com.aa.rac.mod;

import com.aa.rac.mod.codegenerator.FileUtil;
import com.aa.rac.mod.codegenerator.eventhub.EventHubPojoGenerator;
import com.aa.rac.mod.codegenerator.replicated.ReplicatedFileGenerator;
import java.io.IOException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CuratorcodegeneratorApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(CuratorcodegeneratorApplication.class, args);
		String filePath =
				System.getProperty("user.dir").replace('\\', '/')
						+ "/src/main/resources/refunded.txt";

		EventHubPojoGenerator eventHubPojoGenerator = new EventHubPojoGenerator();
		eventHubPojoGenerator.eventHubPojoFileGenerator(filePath);
		System.out.println(FileUtil.getClassFileName(filePath).replace(".java.txt", "").toLowerCase());
		ReplicatedFileGenerator replicatedFileGenerator = new ReplicatedFileGenerator(FileUtil.getClassFileName(filePath).replace(".java.txt", "").toLowerCase());
		replicatedFileGenerator.generateReplicatedFile(filePath);
	}

}
