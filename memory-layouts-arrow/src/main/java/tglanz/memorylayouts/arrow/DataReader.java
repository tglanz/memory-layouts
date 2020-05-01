package tglanz.memorylayouts.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class DataReader {

    private static final Logger logger = LogManager.getLogger(DataReader.class);
    private final InputStream inputStream;

    public DataReader(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public void readData(VectorSchemaRoot root, BufferAllocator allocator) {

        DictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
        try (ArrowReader reader = new ArrowStreamReader(inputStream, allocator)) {
            int batch = 0;
            while (reader.loadNextBatch()) {
                logger.debug("read batch {}", batch);

                for (FieldVector fieldVector: root.getFieldVectors()) {
                    final String name = fieldVector.getName();
                    final Types.MinorType minorType = fieldVector.getMinorType();

                    switch (fieldVector.getMinorType()) {
                        case INT:
                            IntVector vector = (IntVector)fieldVector;

                            logger.debug("int field: {} has {} values and capacity of {}", name, vector.getValueCount());
                            for (int idx = 0; idx < vector.getValueCount(); ++idx) {
                                if (vector.isNull(idx)) {
                                    logger.debug("  - {}: null", idx);
                                } else {
                                    logger.debug("  - {}: {}", idx, vector.get(idx));
                                }
                            }
                            break;
                        default:
                            throw new UnsupportedEncodingException(String.format("minor type: %s", minorType));
                    }
                }
            }
        } catch (IOException ex) {
            logger.fatal("failed to read data", ex);
        }
    }
}
