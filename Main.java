import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Main {

  static String prev = "";

  public static void main(String[] args) {
    try {
      Stream<String> files = Files.lines(Paths.get("../parallelstream/5000000 BT Records.csv"));
      files
        .skip(1)
        .forEach(
          line -> {
            String[] data = line
              .toString()
              .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            for (int i = 0; i < data.length; i++) {
              data[i] = data[i].replaceAll(",", "").replaceAll("\"", "");
            }
            String date = data[0];
            String content = String.join(",", data);
            data = content.toString().split(",");
            if (prev == "") {
              prev = data[data.length - 1];
            } else {
              BigDecimal currentBalance = new BigDecimal(data[data.length - 1]);
              BigDecimal depositValue = new BigDecimal(data[data.length - 3]);
              BigDecimal withdrawalValue = new BigDecimal(
                data[data.length - 2]
              );
              BigDecimal initialBalance = new BigDecimal(prev);
              BigDecimal calculatedBalance = initialBalance
                .add(depositValue)
                .subtract(withdrawalValue);
                prev = data[data.length - 1];
              if (currentBalance.compareTo(calculatedBalance) != 0) {
                System.out.println(
                  content.toString() + "," + calculatedBalance.toString()
                );
              }
            }
          }
        );
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
