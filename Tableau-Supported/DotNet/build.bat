SETLOCAL EnableDelayedExpansion
dotnet build Example.csproj || exit /b !ERRORLEVEL!
