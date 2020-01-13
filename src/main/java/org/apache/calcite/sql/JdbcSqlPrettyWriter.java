package org.apache.calcite.sql;

import org.apache.calcite.sql.pretty.SqlPrettyWriter;

/**
 * @author ozinoviev
 * @since 13.01.2020
 */
public class JdbcSqlPrettyWriter extends SqlPrettyWriter {
    private final JdbcSqlDialect dialect;

    public JdbcSqlPrettyWriter(final JdbcSqlDialect dialect) {
        super(dialect);
        this.dialect = dialect;
    }

    @Override
    public void identifier(final String name,
                           final boolean quoted) {
        if (dialect.identifierNeedsToBeQuoted(name)) {
            //todo: возможно теперь состояние "квотированности" идентификатора доступно в дереве запроса
            // В таком случае необходимо удалить это условие
            super.identifier(name, true);
        }

        super.identifier(name, quoted);
    }
}
