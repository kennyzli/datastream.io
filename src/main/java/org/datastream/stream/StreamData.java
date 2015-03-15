package org.datastream.stream;

import java.util.List;

/**
 * The Stream data which contains the elements for the DataStream
 * 
 * @author kenny.li
 *
 */
public interface StreamData {
    List<String> getColumnsNames();

    List<String> getColumsValues();
}
