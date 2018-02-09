package com.github.msteiger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A line-based buffered reader that skips over empty lines.
 */
public class SkipEmptyLineReader extends Reader {

    private BufferedReader reader;

    /**
     * @param reader a buffered reader
     */
    public SkipEmptyLineReader(BufferedReader reader) {
        this.reader = reader;
    }

    /**
     * Reads the next line of text that is not empty.
     * A line is considered to be terminated by any one
     * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
     * followed immediately by a linefeed.
     *
     * @return     A String containing the contents of the line, not including
     *             any line-termination characters, or null if the end of the
     *             stream has been reached
     *
     * @exception  IOException  If an I/O error occurs
     *
     * @see java.nio.file.Files#readAllLines
     */
    public String readLine() throws IOException {
        String value;
        do {
            value = reader.readLine();
        }
        while (value.isEmpty());
        return value;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return reader.read(cbuf, off, len);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
