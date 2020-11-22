package org.yamcs.yarch.streamsql;

import org.yamcs.yarch.DataType;
import org.yamcs.yarch.FilterableTarget;
import org.yamcs.utils.parser.ParseException;

public class OrExpression extends Expression {

    public OrExpression(Expression left, Expression right) throws ParseException {
        super(new Expression[] { left, right });
    }

    @Override
    public void addFilter(FilterableTarget tableStream) throws StreamSqlException {
        //cannot apply or condition to filter
    }

    @Override
    public void doBind() throws StreamSqlException {
        for (Expression c : children) {
            if (c.getType() != DataType.BOOLEAN) {
                throw new GenericStreamSqlException("'" + c + "' is not of type boolean");
            }
        }
        type = DataType.BOOLEAN;
    }

    @Override
    public void fillCode_getValueReturn(StringBuilder code) throws StreamSqlException {
        boolean first = true;
        code.append("(");
        for (Expression expr : children) {
            if (!first) {
                code.append("||");
            } else {
                first = false;
            }
            expr.fillCode_getValueReturn(code);
        }
        code.append(")");
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (Expression expr : children) {
            if (first) {
                first = false;
            } else {
                sb.append(" OR ");
            }
            sb.append("(");
            sb.append(expr.toString());
            sb.append(")");
        }
        return sb.toString();
    }
}
