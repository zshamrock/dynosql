package com.akazlou.dynosql;

import com.akazlou.dynosql.antlr4.SQLiteBaseListener;
import com.akazlou.dynosql.antlr4.SQLiteParser;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;

class SQLToDynamoDBListener extends SQLiteBaseListener {

    private static final String ALL = "*";

    private final ExpressionSpecBuilder builder;
    private ExpressionType type;
    private String tableName;

    SQLToDynamoDBListener() {
        builder = new ExpressionSpecBuilder();
    }

    @Override
    public void enterSelect_core(final SQLiteParser.Select_coreContext ctx) {
        this.type = ExpressionType.QUERY;
    }

    @Override
    public void enterResult_column(final SQLiteParser.Result_columnContext ctx) {
        final String column = ctx.getText();
        if (column.equals(ALL)) {
            return;
        }
        builder.addProjection(column);
    }

    @Override
    public void enterTable_name(final SQLiteParser.Table_nameContext ctx) {
        this.tableName = ctx.getText();
    }

    @Override
    public void enterExpr(final SQLiteParser.ExprContext ctx) {
        System.out.println(ctx.getText());
    }

    private enum ExpressionType {
        QUERY,
        SCAN
    }
}
