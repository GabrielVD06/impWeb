using System.Diagnostics;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

if (ConnectionSecret.IsProtectRequest(args))
{
    ConnectionSecret.ProtectFromArgs(args, builder.Environment.ContentRootPath, builder.Configuration);
    return;
}

builder.Services.Configure<CopyOptions>(builder.Configuration.GetSection("Copy"));
builder.Services.Configure<PrintOptions>(builder.Configuration.GetSection("Print"));
builder.Services.AddSingleton<PdfCopier>();
builder.Services.AddSingleton<TicketPrinter>();
builder.Services.AddHostedService<CopyWorker>();

await builder.Build().RunAsync();

internal sealed class CopyOptions
{
    public string ConnectionString { get; set; } = "";
    public string ContainerName { get; set; } = "generated-files";

    /// <summary>Guid del local. Vacío copia los PDF nuevos de todos los restaurantes.</summary>
    public string TenantId { get; set; } = "";

    public string LocalFolder { get; set; } = "toPrint";
    public string StateFile { get; set; } = "copy-state.json";
    public int PollIntervalSeconds { get; set; } = 5;
}

internal sealed class CopyWorker : BackgroundService
{
    private readonly PdfCopier _copier;
    private readonly TicketPrinter _printer;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<CopyWorker> _logger;
    private readonly bool _once;

    public CopyWorker(
        PdfCopier copier,
        TicketPrinter printer,
        IHostApplicationLifetime lifetime,
        ILogger<CopyWorker> logger)
    {
        _copier = copier;
        _printer = printer;
        _lifetime = lifetime;
        _logger = logger;
        _once = Environment.GetCommandLineArgs().Any(arg => arg.Equals("--once", StringComparison.OrdinalIgnoreCase));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _copier.StartAsync(stoppingToken);
            _printer.Start();
            do
            {
                await _copier.CopyNewAsync(stoppingToken);
                _printer.DispatchPending();
                if (_once || stoppingToken.IsCancellationRequested)
                    break;

                await Task.Delay(TimeSpan.FromSeconds(_copier.PollIntervalSeconds), stoppingToken);
            }
            while (!stoppingToken.IsCancellationRequested);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Cierre normal.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "El copiado se detuvo.");
        }
        finally
        {
            if (_once)
                _lifetime.StopApplication();
        }
    }
}

internal sealed class PdfCopier
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly CopyOptions _options;
    private readonly IHostEnvironment _environment;
    private readonly ILogger<PdfCopier> _logger;
    private readonly BlobContainerClient _container;
    private readonly string[] _prefixes;
    private readonly string _localFolder;
    private readonly string _statePath;
    private CopyState _state = new();
    private bool _reportedBaseline;

    public int PollIntervalSeconds => Math.Max(2, _options.PollIntervalSeconds);

    public PdfCopier(IOptions<CopyOptions> options, IHostEnvironment environment, ILogger<PdfCopier> logger)
    {
        _options = options.Value;
        _environment = environment;
        _logger = logger;

        var connectionString = ConnectionSecret.Load(_environment.ContentRootPath, _options.ConnectionString, _logger);
        _container = new BlobContainerClient(connectionString, _options.ContainerName);
        _prefixes = BuildPrefixes(_options.TenantId);
        _localFolder = Resolve(_options.LocalFolder);
        _statePath = Resolve(_options.StateFile);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_localFolder);
        _state = await LoadStateAsync(cancellationToken);

        _logger.LogInformation("Carpeta lista: {Folder}", _localFolder);
        if (string.IsNullOrWhiteSpace(_options.TenantId))
        {
            _logger.LogWarning(
                "TenantId está vacío. Un pedido de cualquier restaurante en este storage se copia.");
        }
        else
        {
            _logger.LogInformation("Filtro de tenant {TenantId}.", _options.TenantId);
        }

        _logger.LogInformation(
            "Contenedor {Container}, prefijos {Prefixes}. Se copian PDF con fecha posterior a {Watermark:u}.",
            _options.ContainerName,
            string.Join(", ", _prefixes),
            _state.WatermarkUtc);
    }

    public async Task CopyNewAsync(CancellationToken cancellationToken)
    {
        var pending = new List<BlobItem>();
        var older = 0;

        foreach (var prefix in _prefixes)
        {
            await foreach (var blob in _container.GetBlobsAsync(
                               prefix: prefix,
                               cancellationToken: cancellationToken))
            {
                if (!blob.Name.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
                    continue;

                var modified = blob.Properties.LastModified?.UtcDateTime;
                if (modified is null)
                    continue;

                if (IsNew(modified.Value, blob.Name))
                    pending.Add(blob);
                else
                    older++;
            }
        }

        if (!_reportedBaseline)
        {
            _logger.LogInformation("PDF anteriores que se dejan en Azure: {Count}.", older);
            _reportedBaseline = true;
        }

        pending.Sort(static (left, right) =>
        {
            var byTime = left.Properties.LastModified!.Value.CompareTo(right.Properties.LastModified!.Value);
            return byTime != 0 ? byTime : string.CompareOrdinal(left.Name, right.Name);
        });

        var copied = 0;
        foreach (var blob in pending)
        {
            var modified = blob.Properties.LastModified!.Value.UtcDateTime;
            var fileName = Path.GetFileName(blob.Name);
            if (string.IsNullOrWhiteSpace(fileName))
                continue;

            var destination = Path.Combine(_localFolder, fileName);
            if (File.Exists(destination))
            {
                _logger.LogInformation("Ya estaba en toPrint: {File}", fileName);
                await AdvanceAsync(modified, blob.Name, cancellationToken);
                continue;
            }

            var partial = destination + ".partial";
            try
            {
                await _container.GetBlobClient(blob.Name).DownloadToAsync(partial, cancellationToken);
                File.Move(partial, destination);
                await AdvanceAsync(modified, blob.Name, cancellationToken);
                copied++;
                _logger.LogInformation("Copiado {File}", fileName);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                TryDelete(partial);
                _logger.LogError(ex, "No se pudo copiar {Blob}. Se reintenta en el siguiente ciclo.", blob.Name);
                break;
            }
        }

        if (copied == 0 && pending.Count == 0)
            _logger.LogInformation("Sin PDF nuevos.");
    }

    private bool IsNew(DateTime modifiedUtc, string blobName)
    {
        var byTime = DateTime.Compare(modifiedUtc, _state.WatermarkUtc);
        if (byTime > 0)
            return true;
        if (byTime < 0)
            return false;

        return string.CompareOrdinal(blobName, _state.WatermarkBlobName) > 0;
    }

    private async Task AdvanceAsync(DateTime modifiedUtc, string blobName, CancellationToken cancellationToken)
    {
        _state.WatermarkUtc = modifiedUtc;
        _state.WatermarkBlobName = blobName;
        var json = JsonSerializer.Serialize(_state, JsonOptions);
        var partial = _statePath + ".partial";
        await File.WriteAllTextAsync(partial, json, cancellationToken);
        File.Move(partial, _statePath, overwrite: true);
    }

    private async Task<CopyState> LoadStateAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_statePath))
        {
            var created = new CopyState { WatermarkUtc = DateTime.UtcNow, WatermarkBlobName = "" };
            await AdvanceAsync(created.WatermarkUtc, created.WatermarkBlobName, cancellationToken);
            _logger.LogInformation(
                "Primera ejecución. Los PDF que ya estaban en Azure no se copian.");
            return created;
        }

        await using var stream = File.OpenRead(_statePath);
        var state = await JsonSerializer.DeserializeAsync<CopyState>(stream, cancellationToken: cancellationToken);
        if (state is null || state.WatermarkUtc == default)
            throw new InvalidOperationException($"No se pudo leer {_statePath}.");

        state.WatermarkUtc = DateTime.SpecifyKind(state.WatermarkUtc, DateTimeKind.Utc);
        state.WatermarkBlobName ??= "";
        return state;
    }

    private string Resolve(string path)
    {
        return Path.IsPathRooted(path)
            ? path
            : Path.Combine(_environment.ContentRootPath, path);
    }

    private static string[] BuildPrefixes(string tenantId)
    {
        if (string.IsNullOrWhiteSpace(tenantId))
            return ["orders/", "receipts/"];

        if (!Guid.TryParse(tenantId, out var tenant))
            throw new InvalidOperationException("Copy:TenantId tiene que ser un Guid.");

        var folder = tenant.ToString("N");
        return [$"orders/{folder}/", $"receipts/{folder}/"];
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
            // El siguiente ciclo vuelve a intentar la descarga.
        }
    }
}

internal sealed class CopyState
{
    public DateTime WatermarkUtc { get; set; }
    public string WatermarkBlobName { get; set; } = "";
}

internal sealed class PrintOptions
{
    public bool Simulate { get; set; } = true;
    public string SumatraPath { get; set; } = "";
    public string PrintedFolder { get; set; } = "Printed";
    public string ErrorFolder { get; set; } = "error";
    public Dictionary<string, string> Printers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

internal sealed class TicketPrinter
{
    private readonly PrintOptions _options;
    private readonly IHostEnvironment _environment;
    private readonly ILogger<TicketPrinter> _logger;
    private readonly string _incoming;
    private readonly string _printed;
    private readonly string _failed;

    public TicketPrinter(IOptions<PrintOptions> options, IOptions<CopyOptions> copyOptions, IHostEnvironment environment, ILogger<TicketPrinter> logger)
    {
        _options = options.Value;
        _environment = environment;
        _logger = logger;
        _incoming = Resolve(copyOptions.Value.LocalFolder);
        _printed = Resolve(_options.PrintedFolder);
        _failed = Resolve(_options.ErrorFolder);
    }

    public void Start()
    {
        Directory.CreateDirectory(_incoming);
        Directory.CreateDirectory(_printed);
        Directory.CreateDirectory(_failed);

        if (_options.Simulate)
        {
            _logger.LogInformation(
                "Simulación activa. Los PDF de {Incoming} pasan a {Printed} sin llamar a Sumatra.",
                _incoming,
                _printed);
            return;
        }

        if (!File.Exists(_options.SumatraPath))
            throw new InvalidOperationException($"No se encontró Sumatra en {_options.SumatraPath}.");

        _logger.LogInformation("Sumatra: {Path}", _options.SumatraPath);
    }

    public void DispatchPending()
    {
        var files = Directory.EnumerateFiles(_incoming, "*.pdf")
            .OrderBy(File.GetCreationTimeUtc)
            .ToList();
        foreach (var file in files)
        {
            if (file.EndsWith(".partial", StringComparison.OrdinalIgnoreCase))
                continue;

            var station = StationFromFileName(Path.GetFileName(file));
            var printer = PrinterFor(station);
            if (printer is null)
            {
                Move(file, _failed);
                _logger.LogError(
                    "Estación {Station} sin impresora configurada. {File} quedó en error.",
                    string.IsNullOrWhiteSpace(station) ? "(vacía)" : station,
                    Path.GetFileName(file));
                continue;
            }

            if (_options.Simulate)
            {
                var destination = Move(file, _printed);
                _logger.LogInformation(
                    "Simulado {File}. Estación {Station}, impresora {Printer}. Quedó en {Destination}.",
                    Path.GetFileName(file),
                    station,
                    printer,
                    destination);
                continue;
            }

            if (TryPrint(file, printer))
            {
                Move(file, _printed);
                _logger.LogInformation("Impreso {File} en {Printer}.", Path.GetFileName(file), printer);
            }
            else
            {
                Move(file, _failed);
                _logger.LogError("Falló la impresión de {File} en {Printer}. Quedó en error.", Path.GetFileName(file), printer);
            }
        }
    }

    private bool TryPrint(string file, string printer)
    {
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = _options.SumatraPath,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        process.StartInfo.ArgumentList.Add("-print-to");
        process.StartInfo.ArgumentList.Add(printer);
        process.StartInfo.ArgumentList.Add("-print-settings");
        process.StartInfo.ArgumentList.Add("noscale");
        process.StartInfo.ArgumentList.Add("-silent");
        process.StartInfo.ArgumentList.Add("-exit-when-done");
        process.StartInfo.ArgumentList.Add(file);

        try
        {
            if (!process.Start())
                return false;
            if (!process.WaitForExit(TimeSpan.FromSeconds(60)))
            {
                process.Kill(entireProcessTree: true);
                return false;
            }

            return process.ExitCode == 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Sumatra no pudo arrancar.");
            return false;
        }
    }

    private string? PrinterFor(string station)
    {
        if (string.IsNullOrWhiteSpace(station))
            return null;

        foreach (var pair in _options.Printers)
        {
            if (string.Equals(pair.Key, station, StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrWhiteSpace(pair.Value))
                return pair.Value;
        }

        return null;
    }

    private static string StationFromFileName(string fileName)
    {
        var parts = Path.GetFileNameWithoutExtension(fileName)
            .Split('_', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0)
            return "";

        if (parts[0].Equals("factura", StringComparison.OrdinalIgnoreCase))
            return "RECEIPT";

        if (parts.Length >= 5 &&
            (parts[1].Equals("send", StringComparison.OrdinalIgnoreCase) ||
             parts[1].Equals("cancel", StringComparison.OrdinalIgnoreCase)))
            return parts[^1];

        return parts[0];
    }

    private string Move(string source, string folder)
    {
        var destination = Path.Combine(folder, Path.GetFileName(source));
        if (File.Exists(destination))
            destination = Path.Combine(folder, $"{Path.GetFileNameWithoutExtension(source)}_{DateTime.Now:HHmmssfff}{Path.GetExtension(source)}");

        File.Move(source, destination);
        return destination;
    }

    private string Resolve(string path)
    {
        return Path.IsPathRooted(path)
            ? path
            : Path.Combine(_environment.ContentRootPath, path);
    }
}

[SupportedOSPlatform("windows")]
internal static class ConnectionSecret
{
    public const string FileName = "connection.protected";

    private static readonly byte[] Entropy = Encoding.UTF8.GetBytes("print-copy-v1");

    public static bool IsProtectRequest(string[] args) =>
        args.Any(arg =>
            arg.Equals("--protect", StringComparison.OrdinalIgnoreCase) ||
            arg.Equals("--protect-file", StringComparison.OrdinalIgnoreCase));

    public static void ProtectFromArgs(string[] args, string contentRoot, IConfiguration configuration)
    {
        var fileIndex = Array.FindIndex(
            args,
            arg => arg.Equals("--protect-file", StringComparison.OrdinalIgnoreCase));

        string? sourceFile = null;
        string secret;
        if (fileIndex >= 0)
        {
            if (fileIndex + 1 >= args.Length || args[fileIndex + 1].StartsWith('-'))
                throw new InvalidOperationException("Indique el archivo: dotnet run -- --protect-file ruta\\cadena.txt");

            sourceFile = args[fileIndex + 1];
            if (!File.Exists(sourceFile))
                throw new InvalidOperationException($"No se encontró el archivo de la cadena: {sourceFile}");

            secret = File.ReadAllText(sourceFile).Trim();
        }
        else
        {
            secret = configuration["Copy:ConnectionString"]?.Trim() ?? "";
        }

        if (string.IsNullOrWhiteSpace(secret))
        {
            throw new InvalidOperationException(
                "No hay cadena para cifrar. Póngala un momento en Copy:ConnectionString, o pase un archivo con --protect-file.");
        }

        var plain = Encoding.UTF8.GetBytes(secret);
        try
        {
            var protectedBytes = ProtectedData.Protect(plain, Entropy, DataProtectionScope.LocalMachine);
            var destination = Path.Combine(contentRoot, FileName);
            var partial = destination + ".partial";
            File.WriteAllBytes(partial, protectedBytes);
            File.Move(partial, destination, overwrite: true);
            Console.WriteLine($"Cadena cifrada en {destination}.");
            Console.WriteLine("Solo este equipo puede leerla. Borre Copy:ConnectionString de appsettings.json si todavía está.");
        }
        finally
        {
            Array.Clear(plain);
        }

        if (sourceFile is not null)
            File.Delete(sourceFile);
    }

    public static string Load(string contentRoot, string configured, ILogger logger)
    {
        var path = Path.Combine(contentRoot, FileName);
        if (File.Exists(path))
        {
            if (!string.IsNullOrWhiteSpace(configured))
                logger.LogWarning("Copy:ConnectionString sigue en appsettings.json y no se usa. Bórrelo.");

            logger.LogInformation("Conexión de Azure cifrada con la clave de este equipo.");
            return Unprotect(path);
        }

        if (!string.IsNullOrWhiteSpace(configured))
        {
            logger.LogWarning("La cadena de Azure está en texto plano. En este PC ejecute: dotnet run -- --protect");
            return configured;
        }

        throw new InvalidOperationException(
            "Falta la cadena de Azure. En este PC ejecute: dotnet run -- --protect");
    }

    private static string Unprotect(string path)
    {
        byte[] plain;
        try
        {
            plain = ProtectedData.Unprotect(File.ReadAllBytes(path), Entropy, DataProtectionScope.LocalMachine);
        }
        catch (CryptographicException)
        {
            throw new InvalidOperationException(
                "connection.protected no se puede leer en este equipo. Vuelva a ejecutar --protect aquí.");
        }

        try
        {
            return Encoding.UTF8.GetString(plain);
        }
        finally
        {
            Array.Clear(plain);
        }
    }
}
