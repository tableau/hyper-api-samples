SETLOCAL EnableDelayedExpansion
dotnet build Example.csproj || exit /b !ERRORLEVEL!
xcopy /Y ..\lib\tableauhyperapi.dll bin\Debug\netcoreapp3.1\ || exit /b !ERRORLEVEL!
xcopy /E /Y ..\lib\hyper bin\Debug\netcoreapp3.1\hyper\ || exit /b !ERRORLEVEL!
