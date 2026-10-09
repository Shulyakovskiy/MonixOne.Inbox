using System.Text.Json;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MonixOne.Inbox;
using NATS.Client.Core;

var builder = Host.CreateApplicationBuilder(
    new HostApplicationBuilderSettings { Args = args, ContentRootPath = AppContext.BaseDirectory }
);

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Application"))
);
builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(
    NatsOpts.Default with
    {
        Url = builder.Configuration["Nats:Url"] ?? "nats://localhost:4222",
        AuthOpts = new NatsAuthOpts
        {
            Username = builder.Configuration["Nats:Username"] ?? "admin",
            Password = builder.Configuration["Nats:Password"] ?? "admin"
        },
    }
));

builder
    .Services
    .AddOrderedInbox<AppDbContext>(builder.Configuration.GetSection("OrderedInbox"))
    .AddHandler<LocationsHandler>("locations");

// The application provisions DOMAIN_EVENTS; startup validates the schema and durable consumers.
// Replica callbacks may overlap; handlers keep all effects in the supplied transaction.
using var host = builder.Build();
await host.StartAsync();
host.Services.GetRequiredService<ILoggerFactory>()
    .CreateLogger("Example")
    .LogInformation("Inbox intake and business processing are running for two independent handlers.");
await host.WaitForShutdownAsync();

sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options);

[UsedImplicitly]
sealed class LocationsHandler(InboxDiagnostics diagnostics) : IInboxHandler<AppDbContext>
{
    public Task<InboxResult> HandleAsync(
        AppDbContext db,
        JsonElement message,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // The entry is both an ordinary ILogger event and durable diagnostics of this attempt after COMMIT.
        diagnostics.Write(
            "location.event.ignored",
            "Registration example has no business projection.",
            JsonSerializer.SerializeToElement(new { event_type = message.GetProperty("event_type").GetString() })
        );
        return Task.FromResult(InboxResult.Ignore("Registration example has no business projection."));
    }
}
