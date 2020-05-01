package tglanz.memorylayouts.arrow;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;

public class DataGenerator {

    private static final Logger logger = LogManager.getLogger(DataGenerator.class);
    private final OutputStream outputStream;

    public DataGenerator(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void generateData(VectorSchemaRoot root) {

        DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
        try (ArrowWriter writer = new ArrowStreamWriter(root, dictionaryProvider, outputStream)) {
            writer.start();

            int rowCount = 10;
            root.setRowCount(rowCount);

            final IntVector idVector = (IntVector) root.getVector("id");
            for (int idx = 0; idx < rowCount; ++idx) {
                idVector.setSafe(idx, idx);
            }

            writer.writeBatch();
//
//            root.setRowCount(10);
//            for (int idx = 0; idx < 10; ++idx) {
//                final IntVector idVector = (IntVector) root.getVector("id");
//                idVector.setInitialCapacity(10);
//                idVector.allocateNew();
//
//                idVector.setSafe(1, 5);
//                idVector.setSafe(4, 3);
//
//                writer.writeBatch();
//            }

            writer.end();

            logger.info("{} bytes written", writer.bytesWritten());
        } catch (IOException ex) {
            logger.fatal("failed to generate data", ex);
        }
    }
}
