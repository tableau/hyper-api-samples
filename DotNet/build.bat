SETLOCAL EnableDelayedExpansion
dotnet build Example.csproj || exit /b !ERRORLEVEL!
xcopy /Y ..\lib\tableauhyperapi.dll bin\Debug\netcoreapp2.2\ || exit /b !ERRORLEVEL!
xcopy /E /Y ..\lib\hyper bin\Debug\netcoreapp2.2\hyper\ || exit /b !ERRORLEVEL!
