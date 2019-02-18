package badlog.lib;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class BadLog {

    /**
     * The unit to use when the data does not have a unit.
     */
    public static final String UNITLESS = "ul";
    public static final String DEFAULT_DATA = Double.toString(-1.0);

    private static Optional<BadLog> instance = Optional.empty();

    private boolean registerMode;

    private List<NamespaceObject> namespace;
    private HashMap<String, Optional<String>> publishedData;
    private List<Topic> topics;

    FileOutputStream file;
    private BufferedWriter fileWriter;
    private GZIPOutputStream gzFile;

    private Function<Double, String> doubleStringFunction = (d) -> String.format("%.5g", d);

    protected BadLog(String path, Boolean compress) {
        registerMode = true;
        namespace = new ArrayList<>();
        topics = new ArrayList<>();
        publishedData = new HashMap<>();
        if (compress) {
            try {
                file = new FileOutputStream(new File(path + ".gz"));
                gzFile = new GZIPOutputStream(file, true);
                fileWriter = new BufferedWriter(new OutputStreamWriter(gzFile, "UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (!compress) {
            try {
                file = new FileOutputStream(new File(path));
                fileWriter = new BufferedWriter(new OutputStreamWriter(file));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("Error creating fileWriter");
        }

    }

    /**
     * Initializes BadLog.
     *
     * @param path of bag file
     * @return the instance of BadLog
     * @throws RuntimeException if already initialized
     */
    public static BadLog init(String path, Boolean compress) {
        if (instance.isPresent())
            throw new RuntimeException();

        BadLog badLog = new BadLog(path, compress);
        instance = Optional.of(badLog);

        return badLog;
    }

    public static BadLog init(String path) {
        return init(path, false);
    }



    /**
     * Creates a topic that logs Strings.
     *
     * @param name
     * @param unit
     * @param supplier the function to be called to return the logged data
     * @param attrs    array of topic attributes
     */
    public static void createTopicStr(String name, Supplier<String> supplier) {
        if (!instance.get().registerMode)
            throw new InvalidModeException();
        if (isInNamespace(name))
            throw new DuplicateNameException();

        instance.get().checkName(name);

        QueriedTopic topic = new QueriedTopic(name, supplier);
        instance.get().namespace.add(topic);
        instance.get().topics.add(topic);
    }

    /**
     * Creates a topic that logs doubles.
     * <p>
     * Doubles are converted to strings based on the doubleToString function.
     *
     * @param name
     * @param unit
     * @param supplier the function to be called to return the logged data
     * @param attrs    array of topic attributes
     */
    public static void createTopic(String name, Supplier<Double> supplier) {
        BadLog instance = BadLog.instance.get();
        createTopicStr(name, () -> instance.doubleStringFunction.apply(supplier.get()));
    }

    /**
     * Creates a subscribed topic.
     *
     * @param name
     * @param unit
     * @param dataInferMode the method to use if data has not been published
     * @param attrs         array of topic attributes
     */
    public static void createTopicSubscriber(String name, DataInferMode dataInferMode) {
        if (!instance.get().registerMode)
            throw new InvalidModeException();
        if (isInNamespace(name))
            throw new DuplicateNameException();

        instance.get().checkName(name);

        instance.get().publishedData.put(name, Optional.empty());
        SubscribedTopic topic = new SubscribedTopic(name, dataInferMode);
        instance.get().namespace.add(topic);
        instance.get().topics.add(topic);
    }

    /**
     * Creates a named value.
     *
     * @param name
     * @param value
     */
    public static void createValue(String name, String value) {
        if (!instance.get().registerMode)
            throw new InvalidModeException();
        if (isInNamespace(name))
            throw new DuplicateNameException();

        instance.get().checkName(name);

        instance.get().namespace.add(new Value(name, value));
    }

    /**
     * Publish a string to a topic.
     *
     * @param name
     * @param value
     */
    public static void publish(String name, String value) {
        BadLog tmp = instance.get();
        if (tmp.registerMode)
            throw new InvalidModeException();
        tmp.recievePublishedData(name, value);
    }

    /**
     * Publish a double to a topic.
     *
     * @param name
     * @param value
     */
    public static void publish(String name, double value) {
        publish(name, instance.get().doubleStringFunction.apply(value));
    }

    /**
     * Closes the logger for any new values or topics.
     * Prints bag file headers.
     */
    public void finishInitialization() {
        if (!registerMode)
            throw new InvalidModeException();
        registerMode = false;

        // CSV Header
        StringJoiner joiner = new StringJoiner(",");
        topics.stream().map(Topic::getName).forEach((n) -> joiner.add(n));
        String header = joiner.toString();

        writeLine(header);

    }

    /**
     * Query all queried topics and process published data.
     * <p>
     * This must be called before each call to log
     */
    public void updateTopics() {
        if (registerMode)
            throw new InvalidModeException();

        topics.stream().filter((o) -> o instanceof QueriedTopic).map((o) -> (QueriedTopic) o)
                .forEach(QueriedTopic::refreshValue);

        topics.stream().filter((o) -> o instanceof SubscribedTopic).map((o) -> (SubscribedTopic) o)
                .forEach((t) -> t.handlePublishedData(publishedData.get(t.getName())));

        publishedData.replaceAll((k, v) -> Optional.empty());
    }

    /**
     * Write the values of each topic to the bag file.
     */
    public void log() {
        if (registerMode)
            throw new InvalidModeException();

        StringJoiner joiner = new StringJoiner(",");
        topics.stream().map(Topic::getValue).map(BadLog::escapeCommas).forEach((v) -> joiner.add(v));
        String line = joiner.toString();

        writeLine(line);
    }

    public void setDoubleToStringFunction(Function<Double, String> function) {
        this.doubleStringFunction = function;
    }

    private static String escapeCommas(String in) {
        if (in.contains(",")) {
            return "\"" + in + "\"";
        }
        return in;
    }

    private static boolean isInNamespace(String name) {
        return instance.get().namespace.stream().anyMatch((o) -> o.getName().equals(name));
    }

    private void recievePublishedData(String name, String value) {
        if (publishedData.get(name) == null)
            throw new NullPointerException();

        publishedData.put(name, Optional.of(value));
    }

    private void checkName(String name) {
        for (char c : name.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == ' ' || c == '_' || c == '/')
                continue;

            // Don't crash or throw exception, probably won't cause errors
            System.out.println("Invalid character " + c + " in name " + name);
            return;
        }
    }

    private void writeLine(String lines) {
        try {
            fileWriter.write(lines + System.lineSeparator());
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}