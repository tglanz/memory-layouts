package tglanz.memorylayouts.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;

public class SimpleArrowApp {

    public final static int BATCHES = 3;
    public final static int ROWS = 4;
    public final static float WRITE_RATIO = 0.5f;

    private static final Logger logger = LogManager.getLogger(SimpleArrowApp.class);

    public static void main(String[] args) throws Exception {
        writePhase();
        readPhase();
        updatePhase(1, -1);
        readPhase();
    }

    private static void writePhase() throws Exception {
        logger.info("write phase");
        RootAllocator allocator = new RootAllocator();

        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
            FileOutputStream stream = new FileOutputStream("out/simple.arrow");

            try (ArrowFileWriter writer = new ArrowFileWriter(root, dictionaryProvider, stream.getChannel())) {
                writer.start();


                for (int batch = 0; batch < BATCHES; ++batch) {
                    root.setRowCount(ROWS);
                    IntVector vector = (IntVector)root.getVector("id");

                    for (int row = 0; row < ROWS * WRITE_RATIO; ++row) {
                        int value = row + (int)Math.pow(10, batch);
                        logger.debug("writing, batch={}, row={}, value={}", batch, row, value);
                        vector.set(row, value);

                    }
                    writer.writeBatch();
                }


                writer.end();
            }
        }
    }

    private static void readPhase() throws Exception {
        logger.info("read phase");
        RootAllocator allocator = new RootAllocator();

        DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
        FileInputStream stream = new FileInputStream("out/simple.arrow");

        try (ArrowFileReader reader = new ArrowFileReader(stream.getChannel(), allocator)) {
            final VectorSchemaRoot root = reader.getVectorSchemaRoot();

            int batch = 0;
            while (reader.loadNextBatch()) {
                ++batch;

                for (FieldVector vector : root.getFieldVectors()) {
                    logger.debug("reading vector, batch={}, name={}, count={}, capacity={}", batch, vector.getName(), vector.getValueCount(), vector.getValueCapacity());
                    for (int idx = 0; idx < vector.getValueCount(); ++idx) {

                        if (idx == 9) {
                            ((IntVector) vector).set(0, 9, 123);
                        }

                        Object value = "unknown";
                        if (vector.isNull(idx)) {
                            value = "null";
                        } else {
                            switch (vector.getMinorType()) {
                                case INT:
                                    value = ((IntVector) vector).get(idx);
                            }
                        }
                        logger.debug("  - {}, {}", idx, value);
                    }
                }
            }
        }
    }

    public static void updatePhase(int nullsToFill, int fillValue) throws Exception {
        logger.info("update phase");
        File file = new File("out/simple.arrow");
        FileInputStream inputStream = new FileInputStream(file);
        FileOutputStream outputStream = new FileOutputStream(file, true);

        RootAllocator allocator = new RootAllocator();
        ArrowFileReader reader = new ArrowFileReader(inputStream.getChannel(), allocator);

        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
        ArrowFileWriter writer = new ArrowFileWriter(root, dictionaryProvider, outputStream.getChannel());
        writer.start();

        int batch = 0;
        while (reader.loadNextBatch()) {
            batch++;

            IntVector vector = (IntVector)root.getVector("id");
            logger.debug("checking, batch={}, name={}, values={}, nulls={}", batch, vector.getName(), vector.getValueCount(), vector.getNullCount());


            for (int idx = 0; idx < vector.getValueCount() && nullsToFill > 0; ++idx) {
                if (vector.isNull(idx)) {
                    logger.debug("  - filling at={}, with={}", idx, fillValue);
                    vector.set(idx, fillValue);
                }
            }

            writer.writeBatch();

        }

        writer.end();
    }
}

