set -e
dotnet build Example.csproj
cp -R ../lib/hyper bin/Debug/netcoreapp2.2
cp -R ../lib/libtableauhyperapi.* bin/Debug/netcoreapp2.2/
