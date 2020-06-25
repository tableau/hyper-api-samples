set -e
dotnet build Example.csproj
cp -R ../lib/hyper bin/Debug/netcoreapp3.1
cp -R ../lib/libtableauhyperapi.* bin/Debug/netcoreapp3.1/
