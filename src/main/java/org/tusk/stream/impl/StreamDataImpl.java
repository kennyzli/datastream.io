package org.tusk.stream.impl;

import java.util.ArrayList;
import java.util.List;

import org.tusk.stream.Column;
import org.tusk.stream.StreamData;

import cascading.tuple.TupleEntry;

/**
 * stream data impl
 * 
 * 
 * @author kenny.li
 *
 */
public class StreamDataImpl extends TupleEntry implements StreamData {

    private List<Column> cols;

    public StreamDataImpl(TupleEntry entry) {
        int size = entry.size();
        cols = new ArrayList<Column>(size);
        for (int i = 0; i < size; i++) {
            Column col = new Column();
            col.setName(entry.getFields().get(i).toString());
            col.setData(entry.getObject(i));
            cols.add(col);
        }
    }

    @Override
    public List<Column> getColumns() {
        return cols;
    }

    @Override
    public void setColumns(List<Column> cols) {
        this.cols = cols;
    }

}
