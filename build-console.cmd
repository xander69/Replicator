@TITLE Build Replicator-Console
call mvn clean package -pl ru.xander.replicator:replicator-console -am
mkdir %CD%\build
del %CD%\build\*.* /s /q
mkdir %CD%\build\lib
copy /V /Y %CD%\console\target\replicator\*.* %CD%\build
copy /V /Y %CD%\console\target\replicator\lib\*.* %CD%\build\lib
