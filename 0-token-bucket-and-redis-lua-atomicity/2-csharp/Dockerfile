# ---- build stage ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app
COPY TokenBucketService.csproj ./
RUN dotnet restore
COPY . ./
RUN dotnet publish -c Release -o /out

# ---- runtime stage ----
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /out ./
EXPOSE 3000
ENTRYPOINT ["dotnet", "TokenBucketService.dll"]
