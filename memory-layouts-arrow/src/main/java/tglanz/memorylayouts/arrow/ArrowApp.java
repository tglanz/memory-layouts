package tglanz.memorylayouts.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

public class ArrowApp {
    private static final Logger logger = LogManager.getLogger(ArrowApp.class);

    public static void main(String[] args) {

        logger.info("starting arrow app");

        final Schema schema = createSchema();
        final BufferAllocator allocator = createAllocator();

        DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            new DataGenerator(outputStream).generateData(root);

            final byte[] data = outputStream.toByteArray();
            writeToFile(data, Paths.get("out/data.arrow"));

            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            new DataReader(inputStream).readData(root, allocator);
        }
    }

    private static void writeToFile(byte[] data, Path path) {
        try {
            path.getParent().toFile().mkdirs();
            Files.write(path, data);
        } catch (IOException ex) {
            logger.fatal("failed to write to data to file at: {}", ex, path);
        }
    }

    private static BufferAllocator createAllocator() {
        logger.debug("creating an allocator");
        final AllocationListener listener = new DefaultAllocationListener();
        final long limit = 1 * 1024 * 1024 * 1024;
        return new RootAllocator(listener, limit);
    }

    private static Schema createSchema() {
        logger.debug("creating a schema");
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);

        Iterable<Field> fields = Arrays.asList(idField);
        Map<String, String> metadata = null;

        return new Schema(fields, metadata);
    }
}
