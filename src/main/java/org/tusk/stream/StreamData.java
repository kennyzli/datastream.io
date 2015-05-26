package org.tusk.stream;

import java.util.List;

/**
 * The Stream data which contains the elements for the DataStream
 * 
 * @author kenny.li
 *
 */
public interface StreamData {
    List<Column> getColumns();

    void setColumns(List<Column> cols);
}
