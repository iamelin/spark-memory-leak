package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SparkApp {
    private static final Logger log = LoggerFactory.getLogger(SparkApp.class);

    private static final int THEAD_POOL_SIZE = 1;

    private static final String SPARK_MASTER_URL = "spark://localhost:7077";
    private static final int SPARK_EXECUTOR_CORES = 2;
    private static final String SPARK_EXECUTOR_MEMORY = "1g";

    private static final String DB_HOST = "localhost";
    private static final String DB_PORT = "5432";
    private static final String DB_NAME = "postgres";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final String DB_TABLE = "test_table";

    public static void main(String[] args) {
        log.debug("Starting application...");

        SparkSession sparkSession = SparkSession.builder()
                .appName("Test Application")
                .master(SPARK_MASTER_URL)
                .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
                .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
                // .config("spark.sql.sources.bucketing.autoBucketedScan.enabled", false) // To get rid of the memory leak
                .getOrCreate();

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(THEAD_POOL_SIZE);
        Runnable runnable = () -> {
            log.debug("Run task...");
            processData(sparkSession);
            log.debug("Running task completed");
        };
        executorService.scheduleWithFixedDelay(runnable, 0L, 30L, TimeUnit.SECONDS);
    }

    private static void processData(SparkSession sparkSession) {
        log.debug("Start processing data...");
        Dataset<Row> dataset = sparkSession.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", getConnectionUrl())
                .option("user", DB_USERNAME)
                .option("password", DB_PASSWORD)
                .option("dbtable", DB_TABLE)
                .load();

        log.debug("Persist dataset...");
        // This is the place where the memory leak occurs
        dataset.persist();

        // Do something...
        log.debug("Do something with the persisted dataset");

        // ...and unpersist the dataset
        log.debug("Unpersist dataset...");
        dataset.unpersist();

        log.debug("Processing data completed");
    }

    private static String getConnectionUrl() {
        return "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
    }
}
