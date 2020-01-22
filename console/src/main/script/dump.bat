chcp 1251
set JAVA_HOME=
%JAVA_HOME%/bin/java -classpath ".;lib/*" ru.xander.replicator.console.Main ^
    -cmd dump ^
    -srcDriver oracle.jdbc.OracleDriver ^
    -srcUrl jdbc:oracle:thin:@<host>:1521:<sid> ^
    -srcUser scott ^
    -srcPwd scott ^
    -srcSchema scott ^
    -trgDriver oracle.jdbc.OracleDriver ^
    -trgUrl jdbc:oracle:thin:@<host>:1521:<sid> ^
    -trgUser scott ^
    -trgPwd scott ^
    -trgSchema scott ^
    -dumpTables tableNames
pause