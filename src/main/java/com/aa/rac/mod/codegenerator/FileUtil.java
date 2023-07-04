package com.aa.rac.mod.codegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.util.StringUtils;

public class FileUtil {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static String getFileName(String filePath) {
    int slashIndex = filePath.lastIndexOf('/');
    int dotIndex = filePath.lastIndexOf('.');
    if (slashIndex == -1 || dotIndex == -1) {
      throw new IllegalArgumentException("Filepath not supported to extract file name.");
    }
    String fileName = filePath.substring(slashIndex+1, dotIndex).replaceAll("[^A-Za-z]", "_");
    String[] fileSplits = fileName.split("_");
    String finalFileName = "";
    for (String fileSplit: fileSplits) {
      finalFileName += StringUtils.capitalize(fileSplit);
    }
    return finalFileName;
  }

  public static String getClassFileName(String filePath) {
    return getFileName(filePath) + ".java.txt";
  }

  public static List<String> readLinesFromFile(String filePath) throws IOException {
    Path path = Paths.get(filePath);
    byte[] bytes = Files.readAllBytes(path);
    List<String> allLines = Files.readAllLines(path, StandardCharsets.UTF_8);
    return allLines;
  }

  public static Map<String, Object> mapContentsToHashMap(String fileContents)
      throws JsonProcessingException {
    return objectMapper.readValue(fileContents, HashMap.class);
  }

  public static void createDirectory(String folderPath) {
    File directory = new File(folderPath);
    if (!directory.exists()) {
      directory.mkdirs();
      System.out.println("Directory created: " + directory);
    } else {
      System.out.println("Directory already exists.");
    }
  }

  public static void createFile(String ehDirectory, String fullFilePath) throws IOException {
    File eventHubFile = new File(fullFilePath);
    createDirectory(ehDirectory);
    if (eventHubFile.createNewFile()) {
      System.out.println("File created: " + eventHubFile.getName());
    } else {
      System.out.println("File already exists.");
    }
  }

  public static String getFieldName(String field) {
    field = field.replaceAll("[^A-Za-z]", "_");
    String[] splits = field.split("_");
    String suffix = splits.length !=0 && splits[0].equalsIgnoreCase("b") ? "Before" : "";
    String finalField = "";
    if (!suffix.isBlank()) {
      splits = Arrays.copyOfRange(splits, 1, splits.length);
    }
    int i = 0;
    for (String split: splits) {
      if (i == 0) {
        finalField += split.toLowerCase();
        i += 1;
        continue;
      }
      finalField += StringUtils.capitalize(split.toLowerCase());
    }
    return finalField + suffix;
  }
}
