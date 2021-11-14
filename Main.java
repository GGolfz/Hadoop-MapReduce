import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.math.BigDecimal;
public class Main {

  static String prev = "";

  public static void main(String[] args) {
    try {
      Stream<String> files = Files.lines(Paths.get("5000 BT Records.csv"));
      files.skip(1).map(
          line -> {
            String[] data = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            for (int i = 0; i < data.length; i++) {
              data[i] = data[i].replaceAll(",","").replaceAll("\"", "");
            }
            return String.join(",",data);
          }
        )
        .map(
          line -> {
            String[] data = line.split(",");
            if (prev == "") {
              BigDecimal currentBalance = new BigDecimal(data[data.length - 1]);
              BigDecimal depositValue = new BigDecimal(data[data.length - 3]);
              BigDecimal withdrawalValue = new BigDecimal(data[data.length - 2]);
              String result = line + "," + (currentBalance.add(withdrawalValue).subtract(depositValue)).toString();
              prev = data[data.length - 1];
              return result;
            } else {
              String result = line + "," + prev;
              prev = data[data.length - 1];
              return result;
            }
          }
        ).map(line -> {
          String[] data = line.split(",");
          BigDecimal initialBalance = new BigDecimal(data[data.length - 1]);
          BigDecimal depositValue = new BigDecimal(data[data.length - 4]);
          BigDecimal withdrawalValue = new BigDecimal(data[data.length - 3]);
          BigDecimal calculatedBalance = initialBalance.add(depositValue).subtract(withdrawalValue);
          BigDecimal finalBalance = new BigDecimal(data[data.length - 2]);
          return data[0]+","+data[1]+","+finalBalance+","+calculatedBalance;
        }).filter(line -> {
          String[] data = line.split(",");
          BigDecimal finalBalance = new BigDecimal(data[2]);
          BigDecimal calculatedBalance = new BigDecimal(data[3]);
          return calculatedBalance.compareTo(finalBalance) != 0;
        })
        .forEach(System.out::println);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
