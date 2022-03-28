# This script uses a local copy of the Hyper API package to build the example project. Use "build-nuget.sh" to download the package from nuget instead.
set -e
dotnet build Example.csproj
cp -R ../lib/hyper bin/Debug/netcoreapp3.1
cp -R ../lib/libtableauhyperapi.* bin/Debug/netcoreapp3.1/
