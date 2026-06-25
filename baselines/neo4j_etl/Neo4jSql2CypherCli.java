import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.stream.Collectors;

public class Neo4jSql2CypherCli {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            throw new IllegalArgumentException(
                    "Usage: Neo4jSql2CypherCli <jdbcUrl> <user> <password> <tableMappings> <joinMappings>");
        }
        var props = new Properties();
        props.put("username", args[1]);
        props.put("password", args[2]);
        props.put("enableSQLTranslation", "true");
        props.put("s2c.sqlDialect", "SQLITE");
        props.put("s2c.prettyPrint", "true");
        props.put("s2c.alwaysEscapeNames", "true");
        props.put("s2c.parseNameCase", "AS_IS");
        if (!args[3].isBlank()) {
            props.put("s2c.tableToLabelMappings", args[3]);
        }
        if (!args[4].isBlank()) {
            props.put("s2c.joinColumnsToTypeMappings", args[4]);
        }
        var sql = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        try (var connection = DriverManager.getConnection(args[0], props)) {
            System.out.print(connection.nativeSQL(sql));
        }
    }
}
