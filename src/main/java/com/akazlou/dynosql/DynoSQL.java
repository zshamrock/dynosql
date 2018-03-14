package com.akazlou.dynosql;

import com.akazlou.dynosql.antlr4.SQLiteLexer;
import com.akazlou.dynosql.antlr4.SQLiteParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class DynoSQL {
    public static void main(final String[] args) {
        new DynoSQL().exec("select x, y from T where ID = 1");
    }

    public void exec(final String query) {
        final CodePointCharStream stream = CharStreams.fromString(query);
        final SQLiteLexer lexer = new SQLiteLexer(stream);
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final SQLiteParser parser = new SQLiteParser(tokens);
        final ParseTree tree = parser.parse();
        final ParseTreeWalker walker = new ParseTreeWalker();
        final SQLToDynamoDBListener listener = new SQLToDynamoDBListener();
        walker.walk(listener, tree);
    }
}
