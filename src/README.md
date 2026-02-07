<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <AzureFunctionsVersion>v4</AzureFunctionsVersion>
    <OutputType>Exe</OutputType>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <!-- Azure Functions and Durable Task Framework -->
    <PackageReference Include="Microsoft.Azure.Functions.Worker" Version="1.20.0" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Sdk" Version="1.15.1" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Extensions.DurableTask" Version="1.1.1" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Extensions.Storage" Version="6.0.0" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Extensions.Http" Version="3.2.0" />
    <PackageReference Include="Microsoft.Extensions.Azure" Version="1.7.0" />

    <!-- Azure SDK Libraries -->
    <PackageReference Include="Azure.Storage.Blobs" Version="12.19.0" />
    <PackageReference Include="Azure.Data.Tables" Version="12.8.0" />
    
    <!-- Microsoft Graph for SharePoint Integration -->
    <PackageReference Include="Microsoft.Graph" Version="5.40.0" />
    <PackageReference Include="Azure.Identity" Version="1.13.0" />

    <!-- Logging and Configuration -->
    <PackageReference Include="Microsoft.Extensions.Logging" Version="8.0.0" />
    <PackageReference Include="Microsoft.Extensions.Configuration" Version="8.0.0" />

    <!-- PDF Processing (iText7 - AGPL-3.0 for POC, Commercial License for Production) -->
    <PackageReference Include="itext7" Version="8.0.0" />
    <PackageReference Include="itext7.bouncy-castle-adapter" Version="8.0.0" />

    <!-- Azure AI Document Intelligence (formerly Form Recognizer) -->
    <PackageReference Include="Azure.AI.FormRecognizer" Version="4.1.0" />
  </ItemGroup>

  <ItemGroup>
    <None Update="host.json">
      <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
    </None>
    <None Update="local.settings.json">
      <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
      <CopyToPublishDirectory>Never</CopyToPublishDirectory>
    </None>
  </ItemGroup>
</Project>
-------------------------
{
  "version": "2.0",
  "logging": {
    "logLevel": {
      "default": "Debug",
      "Microsoft.Azure.WebJobs": "Debug",
      "Microsoft.Azure.WebJobs.Logging": "Information"
    },
    "fileLoggingMode": "always",
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "maxTelemetryItemsPerSecond": 20
      }
    }
  },
  "functionTimeout": "00:30:00",
  "durableTask": {
    "tracing": {
      "traceInputsAndOutputs": true
    },
    "maxConcurrentActivityFunctions": 10,
    "maxConcurrentOrchestratorFunctions": 10
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[4.*, 5.0.0)"
  }
}
----------------
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
    "STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=",
    "INCOMING_CONTAINER_NAME": "incoming-loan-packets",
    "SPLIT_CONTAINER_NAME": "split-documents",
    "RESULTS_CONTAINER_NAME": "results",
    "DUPLICATES_CONTAINER_NAME": "duplicates",
    "DOCUMENT_INDEX_TABLE_NAME": "DocumentIndex",
    "DOCUMENT_INTELLIGENCE_ENDPOINT": "",
    "DOCUMENT_INTELLIGENCE_KEY": "",
 
    "SHAREPOINT_TENANT_ID": "",
    "SHAREPOINT_CLIENT_ID": "",
    "SHAREPOINT_CLIENT_SECRET": "",
    "SHAREPOINT_SITE_URL": "",
    "SHAREPOINT_LIBRARY_NAME": ""
  }
}
-----------------------
using DocumentProcessing.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureLogging(logging =>
    {
        // Enable Azure App Service file logging
        // Logs will be written to: D:\home\LogFiles\Application\
        logging.SetMinimumLevel(LogLevel.Debug);
        
        // Add console logging for visibility
        logging.AddConsole();
    })
    .ConfigureServices(services =>
    {
        // Register document boundary detector with configuration and logging
        services.AddScoped<DocumentBoundaryDetector>(provider =>
        {
            var config = new DocumentBoundaryDetector.DetectionConfig
            {
                MinBoundaryConfidence = 0.5,
                RequireStrongTitle = false,
                MinPagesPerDocument = 1,
                MaxTextPreviewLength = 2000
            };
            var logger = provider.GetService<ILogger<DocumentBoundaryDetector>>();
            return new DocumentBoundaryDetector(config, logger);
        });

        // Register application services with logging support
        services.AddScoped<DocumentSplitterService>();
        services.AddScoped<HashingService>();
        services.AddScoped<DeduplicationService>();

        // Register Document Intelligence service
        services.AddScoped<DocumentIntelligenceService>(provider =>
        {
            var config = provider.GetRequiredService<IConfiguration>();
            var endpoint = config["DOCUMENT_INTELLIGENCE_ENDPOINT"] 
                ?? throw new InvalidOperationException("DOCUMENT_INTELLIGENCE_ENDPOINT is missing from app settings");
            var apiKey = config["DOCUMENT_INTELLIGENCE_KEY"] 
                ?? throw new InvalidOperationException("DOCUMENT_INTELLIGENCE_KEY is missing from app settings");
            var logger = provider.GetService<ILogger<DocumentIntelligenceService>>();
            
            return new DocumentIntelligenceService(endpoint, apiKey, logger);
        });

        // Register Enhanced Boundary Detector
        services.AddScoped<EnhancedBoundaryDetectorService>();

        // Register Business Metrics Logger (logs to Azure Application Insights)
        services.AddSingleton<BusinessMetricsLogger>();

        // Register storage service as singleton since connection strings don't change.
        services.AddSingleton<StorageService>(provider =>
        {
            var config = provider.GetRequiredService<IConfiguration>();
            var connectionString = config["STORAGE_CONNECTION_STRING"] 
                ?? throw new InvalidOperationException("STORAGE_CONNECTION_STRING is missing from app settings");
            var incomingContainer = config["INCOMING_CONTAINER_NAME"] 
                ?? throw new InvalidOperationException("INCOMING_CONTAINER_NAME is missing from app settings");
            var splitContainer = config["SPLIT_CONTAINER_NAME"] 
                ?? throw new InvalidOperationException("SPLIT_CONTAINER_NAME is missing from app settings");
            var resultsContainer = config["RESULTS_CONTAINER_NAME"] 
                ?? throw new InvalidOperationException("RESULTS_CONTAINER_NAME is missing from app settings");
            var duplicatesContainer = config["DUPLICATES_CONTAINER_NAME"] 
                ?? throw new InvalidOperationException("DUPLICATES_CONTAINER_NAME is missing from app settings");
            var indexTable = config["DOCUMENT_INDEX_TABLE_NAME"] 
                ?? throw new InvalidOperationException("DOCUMENT_INDEX_TABLE_NAME is missing from app settings");

            return new StorageService(
                connectionString,
                incomingContainer,
                splitContainer,
                resultsContainer,
                duplicatesContainer,
                indexTable,
                provider.GetService<ILogger<StorageService>>()
            );
        });

        // Register SharePoint Sync Service (optional - gracefully disabled if not configured)
        services.AddSingleton<SharePointSyncService>(provider =>
        {
            var config = provider.GetRequiredService<IConfiguration>();

            // All SharePoint settings are optional
            var tenantId = config["SHAREPOINT_TENANT_ID"];
            var clientId = config["SHAREPOINT_CLIENT_ID"];
            var clientSecret = config["SHAREPOINT_CLIENT_SECRET"];
            var siteUrl = config["SHAREPOINT_SITE_URL"];
            // ✅ FIX: Handle empty string, not just null
            var libraryName = string.IsNullOrWhiteSpace(config["SHAREPOINT_LIBRARY_NAME"])
                ? "ProcessedDocuments"
                : config["SHAREPOINT_LIBRARY_NAME"];

            return new SharePointSyncService(
                tenantId,
                clientId,
                clientSecret,
                siteUrl,
                libraryName,
                provider.GetRequiredService<ILogger<SharePointSyncService>>()
            );
        });

        // Durable Task support is automatically enabled via ConfigureFunctionsWorkerDefaults()
    })
    .Build();

host.Run();
------------------
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
    
    "STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=youraccount;AccountKey=yourkey;EndpointSuffix=core.windows.net",
    "INCOMING_CONTAINER_NAME": "incoming-loan-packets",
    "SPLIT_CONTAINER_NAME": "split-documents",
    "RESULTS_CONTAINER_NAME": "results",
    "DUPLICATES_CONTAINER_NAME": "duplicates",
    "DOCUMENT_INDEX_TABLE_NAME": "DocumentIndex",
    
    "DOCUMENT_INTELLIGENCE_ENDPOINT": "https://your-doc-intelligence.cognitiveservices.azure.com/",
    "DOCUMENT_INTELLIGENCE_KEY": "your-doc-intelligence-key",
    
    "SHAREPOINT_TENANT_ID": "",
    "SHAREPOINT_CLIENT_ID": "",
    "SHAREPOINT_CLIENT_SECRET": "",
    "SHAREPOINT_SITE_URL": "",
    "SHAREPOINT_LIBRARY_NAME": "ProcessedDocuments"
  }
}
----------------------
Microsoft Visual Studio Solution File, Format Version 12.00
# Visual Studio Version 17
VisualStudioVersion = 17.5.2.0
MinimumVisualStudioVersion = 10.0.40219.1
Project("{2150E333-8FDC-42A3-9474-1A3956D46DE8}") = "src", "src", "{827E0CD3-B72D-47B6-A68D-7590B98EB39B}"
EndProject
Project("{2150E333-8FDC-42A3-9474-1A3956D46DE8}") = "tests", "tests", "{0AB3BF05-4346-4AA6-1389-037BE0695223}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "DocumentProcessing", "src\DocumentProcessing.csproj", "{F658FFB5-C0F4-99E2-EA74-7C8F854AB3F9}"
EndProject
Project("{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}") = "DocumentProcessing.Tests", "tests\DocumentProcessing.Tests.csproj", "{CC957424-4190-B66E-E202-32FEDCC25D5B}"
EndProject
Global
	GlobalSection(SolutionConfigurationPlatforms) = preSolution
		Debug|Any CPU = Debug|Any CPU
		Release|Any CPU = Release|Any CPU
	EndGlobalSection
	GlobalSection(ProjectConfigurationPlatforms) = postSolution
		{F658FFB5-C0F4-99E2-EA74-7C8F854AB3F9}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{F658FFB5-C0F4-99E2-EA74-7C8F854AB3F9}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{F658FFB5-C0F4-99E2-EA74-7C8F854AB3F9}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{F658FFB5-C0F4-99E2-EA74-7C8F854AB3F9}.Release|Any CPU.Build.0 = Release|Any CPU
		{CC957424-4190-B66E-E202-32FEDCC25D5B}.Debug|Any CPU.ActiveCfg = Debug|Any CPU
		{CC957424-4190-B66E-E202-32FEDCC25D5B}.Debug|Any CPU.Build.0 = Debug|Any CPU
		{CC957424-4190-B66E-E202-32FEDCC25D5B}.Release|Any CPU.ActiveCfg = Release|Any CPU
		{CC957424-4190-B66E-E202-32FEDCC25D5B}.Release|Any CPU.Build.0 = Release|Any CPU
	EndGlobalSection
	GlobalSection(SolutionProperties) = preSolution
		HideSolutionNode = FALSE
	EndGlobalSection
	GlobalSection(NestedProjects) = preSolution
		{F658FFB5-C0F4-99E2-EA74-7C8F854AB3F9} = {827E0CD3-B72D-47B6-A68D-7590B98EB39B}
		{CC957424-4190-B66E-E202-32FEDCC25D5B} = {0AB3BF05-4346-4AA6-1389-037BE0695223}
	EndGlobalSection
	GlobalSection(ExtensibilityGlobals) = postSolution
		SolutionGuid = {95F865C2-32AB-40E5-A940-177055D4EACB}
	EndGlobalSection
EndGlobal
