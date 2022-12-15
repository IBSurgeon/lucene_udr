set ARCH_DIR=C:\Program Files\7-Zip
set OUTPUT_DIR=%1build
set BUILD_DIR=%1build\windows-x64\Release

del "%OUTPUT_DIR%\LuceneUdr_Win_x64.zip"
"%ARCH_DIR%\7z.exe" a -tzip "%OUTPUT_DIR%\LuceneUdr_Win_x64.zip" "%BUILD_DIR%\LuceneUdr.dll" "%BUILD_DIR%\lucene++.dll" "%BUILD_DIR%\lucene++-contrib.dll" ^
 "README.md" "README_RUS.md" "sql"

