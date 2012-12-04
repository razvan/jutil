package json;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.File;
import java.io.IOException;

/*
    Grep Json file (args[1]) for field name (args[0]) and print field value.

    $ mvn exec:java -Dexec.mainClass=json.Grep -Dexec.args='productname /Users/razvan/Downloads/alatest.json'
 */
public class Grep {
    private final String fieldName;
    private final File file;

    private static final JsonFactory jsonFactory = new JsonFactory();

    public Grep(String fieldName, File file) {
        this.fieldName = fieldName;
        this.file = file;
    }

    public static void main(String[] args) {
        final String fieldName = args[0];
        final File file = new File(args[1]);
        new Grep(fieldName, file).run();
    }

    private void run() {
        JsonParser parser = null;
        try {
            parser = jsonFactory.createJsonParser(file);
            while (parser.nextToken() != JsonToken.NOT_AVAILABLE) {
                if (fieldName.equals(parser.getCurrentName())) {
                    parser.nextToken();
                    System.out.println(parser.getText());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (parser != null)
                try {
                    parser.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
}
