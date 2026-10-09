using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MonixOne.Inbox.Registration;
using NATS.Client.Core;

namespace MonixOne.Inbox.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task Two_handlers_have_independent_settings_and_subscriptions()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"))
            .AddHandler<TestHandler>(
                "products",
                h =>
                {
                    Subscribe(h, "products");
                    h.Retry.MaxAttempts = 3;
                    h.GapTimeout = TimeSpan.FromSeconds(3);
                }
            );

        await using var provider = BuildProvider(services);
        var handlers = Catalog(provider).Handlers;
        Assert.Equal(2, handlers.Length);
        Assert.Equal("locations", handlers[0].Id);
        Assert.Equal("products", handlers[1].Id);
        Assert.Equal(5, handlers[0].Processing.Retry.MaxAttempts);
        Assert.Equal(3, handlers[1].Processing.Retry.MaxAttempts);
        Assert.NotSame(handlers[0].Processing.Retry, handlers[1].Processing.Retry);
        Assert.Equal(TimeSpan.FromSeconds(5), handlers[0].Processing.GapTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), handlers[1].Processing.GapTimeout);
        Assert.Equal("events.locations.>", Assert.Single(handlers[0].Subscriptions).Subject);
        Assert.Equal("events.products.>", Assert.Single(handlers[1].Subscriptions).Subject);
    }

    [Fact]
    public async Task Configuration_and_code_produce_the_same_effective_settings()
    {
        var configured = CreateServices();
        configured
            .AddOrderedInbox<TestDb>(
                JsonConfiguration(
                    """
                    {
                      "Schema": "application_inbox",
                      "AutoMigrate": false,
                      "Defaults": { "HandlerTimeout": "00:00:45", "Retry": { "MaxAttempts": 7 } },
                      "Handlers": {
                        "locations": {
                          "MaxParallelObjects": 2,
                          "Subscriptions": {
                            "locations-events": {
                                "Stream": "DOMAIN_EVENTS",
                                "Subject": "events.locations.>",
                                "DurableName": "example-locations"
                            }
                          }
                        }
                      }
                    }
                    """
                )
            )
            .AddHandler<TestHandler>("locations");

        var coded = CreateServices();
        coded
            .AddOrderedInbox<TestDb>(o =>
            {
                o.Schema = "application_inbox";
                o.AutoMigrate = false;
                o.Defaults.HandlerTimeout = TimeSpan.FromSeconds(45);
                o.Defaults.Retry.MaxAttempts = 7;
            })
            .AddHandler<TestHandler>(
                "locations",
                h =>
                {
                    h.MaxParallelObjects = 2;
                    Subscribe(h, "locations");
                }
            );

        await using var configuredProvider = BuildProvider(configured);
        await using var codedProvider = BuildProvider(coded);
        var a = Catalog(configuredProvider);
        var b = Catalog(codedProvider);
        Assert.Equal(a.Settings, b.Settings);
        Assert.Equal(a.Handlers[0].Processing, b.Handlers[0].Processing);
        Assert.Equal(a.Handlers[0].Subscriptions.ToArray(), b.Handlers[0].Subscriptions.ToArray());
    }

    [Fact]
    public async Task Partial_code_overrides_preserve_configuration_and_apply_last()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>(
                JsonConfiguration(
                    """
                    {
                      "Schema": "custom_inbox",
                      "AutoMigrate": false,
                      "Defaults": { "Retry": { "InitialDelay": "00:00:02", "MaxDelay": "00:02:00" } },
                      "Handlers": {
                        "locations": {
                          "HandlerTimeout": "00:00:40",
                          "Retry": { "MaxAttempts": 3 },
                          "Subscriptions": {
                            "locations-events": {
                                "Stream": "DOMAIN_EVENTS",
                                "Subject": "events.locations.>",
                                "DurableName": "example-locations"
                            }
                          }
                        }
                      }
                    }
                    """
                ),
                o => o.Defaults.Retry.MaxAttempts = 8
            )
            .AddHandler<TestHandler>("locations", h => h.PollInterval = TimeSpan.FromSeconds(2))
            .AddSubscription("locations", "locations-events", s => s.BatchSize = 8);

        await using var provider = BuildProvider(services);
        var catalog = Catalog(provider);
        var handler = Assert.Single(catalog.Handlers);
        Assert.Equal("custom_inbox", catalog.Settings.Schema);
        Assert.False(catalog.Settings.AutoMigrate);
        Assert.Equal(3, handler.Processing.Retry.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), handler.Processing.Retry.InitialDelay);
        Assert.Equal(TimeSpan.FromMinutes(2), handler.Processing.Retry.MaxDelay);
        Assert.Equal(TimeSpan.FromSeconds(40), handler.Processing.HandlerTimeout);
        Assert.Equal(TimeSpan.FromSeconds(2), handler.Processing.PollInterval);
        Assert.Equal(4, handler.Processing.MaxParallelObjects);
        var subscription = Assert.Single(handler.Subscriptions);
        Assert.Equal("DOMAIN_EVENTS", subscription.Stream);
        Assert.Equal("events.locations.>", subscription.Subject);
        Assert.Equal(8, subscription.BatchSize);
    }

    [Fact]
    public async Task Handler_code_has_priority_over_common_code_and_configured_handler()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>(
                JsonConfiguration(
                    """
                    { "Handlers": { "locations": { "Retry": { "MaxAttempts": 2 } } } }
                    """
                ),
                o =>
                {
                    o.Defaults.Retry.MaxAttempts = 8;
                    o.Handlers["locations"] = new() { Retry = new() { MaxAttempts = 6 } };
                }
            )
            .AddHandler<TestHandler>(
                "locations",
                h =>
                {
                    Subscribe(h, "locations");
                    h.Retry.MaxAttempts = 4;
                }
            );

        await using var provider = BuildProvider(services);
        Assert.Equal(4, Assert.Single(Catalog(provider).Handlers).Processing.Retry.MaxAttempts);
    }

    [Fact]
    public async Task Code_subscription_updates_configured_transport_without_resetting_batch_size()
    {
        var configuration = JsonConfiguration("""
            { "Handlers": { "locations": { "Subscriptions": { "events": {
                "Stream": "ORIGINAL", "Subject": "old.>", "DurableName": "original", "BatchSize": 17
            } } } } }
            """);
        var services = CreateServices();
        services.AddOrderedInbox<TestDb>(configuration).AddHandler<TestHandler>("locations", h =>
            h.Subscribe("events", "EVENTS", "events.>", "locations"));
        await using var provider = BuildProvider(services);
        var subscription = Assert.Single(Assert.Single(Catalog(provider).Handlers).Subscriptions);
        Assert.Equal("EVENTS", subscription.Stream);
        Assert.Equal("events.>", subscription.Subject);
        Assert.Equal("locations", subscription.DurableName);
        Assert.Equal(17, subscription.BatchSize);
    }

    [Fact]
    public async Task Repeated_registration_adds_handlers_and_only_one_startup_service()
    {
        var services = CreateServices();
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("products", h => Subscribe(h, "products"));
        Assert.Equal(5, services.Count(d => d.ServiceType == typeof(IHostedService)));

        await using var provider = BuildProvider(services);
        Assert.Equal(2, Catalog(provider).Handlers.Length);
        Assert.Same(Catalog(provider), Catalog(provider));
    }

    [Fact]
    public async Task Extra_subscription_from_another_registration_belongs_to_the_existing_handler()
    {
        var services = CreateServices();
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));
        services
            .AddOrderedInbox<TestDb>()
            .AddSubscription(
                "locations",
                "location-deletions",
                s =>
                {
                    s.Stream = "DOMAIN_EVENTS";
                    s.Subject = "events.locations.deleted";
                    s.DurableName = "example-location-deletions";
                }
            );

        await using var provider = BuildProvider(services);
        var handler = Assert.Single(Catalog(provider).Handlers);
        Assert.Equal("locations", handler.Id);
        Assert.Equal(2, handler.Subscriptions.Length);
    }

    [Fact]
    public async Task Subscription_can_be_registered_before_its_handler()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddSubscription(
                "locations",
                "events",
                s =>
                {
                    s.Stream = "EVENTS";
                    s.Subject = "events.>";
                    s.DurableName = "example";
                }
            );
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations");

        await using var provider = BuildProvider(services);
        Assert.Single(Assert.Single(Catalog(provider).Handlers).Subscriptions);
    }

    [Theory]
    [InlineData("locations")]
    [InlineData("Locations")]
    public void Duplicate_handler_id_is_rejected(string duplicate)
    {
        var inbox = CreateServices().AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations");
        Assert.Throws<ArgumentException>(() => inbox.AddHandler<TestHandler>(duplicate));
    }

    [Fact]
    public void Duplicate_explicit_subscription_is_rejected()
    {
        var inbox = CreateServices().AddOrderedInbox<TestDb>().AddSubscription("locations", "events", _ => { });
        Assert.Throws<ArgumentException>(() => inbox.AddSubscription("locations", "Events", _ => { }));
    }

    [Fact]
    public async Task Unknown_configured_handler_fails_host_start_before_other_services_start()
    {
        var builder = Host.CreateApplicationBuilder();
        AddApplicationServices(builder.Services);
        var observer = new StartupObserver();
        builder.Services.AddSingleton<IHostedService>(observer);
        builder
            .Services.AddOrderedInbox<TestDb>(
                JsonConfiguration(
                    """
                    { "Handlers": { "typo": { "Enabled": false } } }
                    """
                )
            )
            .AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));

        using var host = builder.Build();
        var error = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.Contains("unknown handler 'typo'", error.Message);
        Assert.False(observer.Started);
    }

    [Fact]
    public async Task Unknown_subscription_handler_fails_validation()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"))
            .AddSubscription("typo", "events", _ => { });

        await using var provider = BuildProvider(services);
        Assert.Contains("unknown handler", Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("events..locations")]
    [InlineData("events.>.locations")]
    [InlineData("events.loc*")]
    [InlineData("events.loc>")]
    [InlineData("events locations")]
    [InlineData("events.\nlocations")]
    public async Task Invalid_subject_is_rejected(string subject)
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>(
                "locations",
                h =>
                {
                    Subscribe(h, "locations");
                    h.Subscriptions["locations-events"].Subject = subject;
                }
            );

        await using var provider = BuildProvider(services);
        Assert.Contains("Subject", Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message);
    }

    [Theory]
    [InlineData("events.locations.created")]
    [InlineData("events.*.created")]
    [InlineData("events.>")]
    [InlineData(">")]
    [InlineData("events.tenant/42.*")]
    public async Task Valid_subject_filters_are_accepted(string subject)
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>(
                "locations",
                h =>
                {
                    Subscribe(h, "locations");
                    h.Subscriptions["locations-events"].Subject = subject;
                }
            );

        await using var provider = BuildProvider(services);
        Assert.Equal(subject, Assert.Single(Assert.Single(Catalog(provider).Handlers).Subscriptions).Subject);
    }

    [Fact]
    public async Task Different_handlers_cannot_share_one_durable_consumer()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>("locations", h => h.Subscribe("events", "EVENTS", "events.>", "shared"))
            .AddHandler<TestHandler>("products", h => h.Subscribe("events", "EVENTS", "events.>", "shared"));

        await using var provider = BuildProvider(services);
        Assert.Contains(
            "independent durable",
            Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message
        );
    }

    [Fact]
    public async Task One_subject_and_two_handlers_with_independent_durables_are_allowed()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>("locations", h => h.Subscribe("events", "EVENTS", "events.>", "locations"))
            .AddHandler<TestHandler>("products", h => h.Subscribe("events", "EVENTS", "events.>", "products"));

        await using var provider = BuildProvider(services);
        Assert.Equal(2, Catalog(provider).Handlers.Length);
    }

    [Fact]
    public async Task Disabling_one_handler_does_not_disable_another_or_require_its_connection()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>("locations", h => h.Enabled = false)
            .AddHandler<TestHandler>("products", h => Subscribe(h, "products"));

        await using var provider = BuildProvider(services);
        Assert.False(Catalog(provider).Handlers[0].Enabled);
        Assert.True(Catalog(provider).Handlers[1].Enabled);
    }

    [Fact]
    public async Task Type_handler_and_its_dependencies_are_resolved_in_the_operation_scope()
    {
        var services = CreateServices();
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));
        await using var provider = BuildProvider(services);
        var descriptor = Assert.Single(Catalog(provider).Handlers);

        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        var firstHandler = Assert.IsType<TestHandler>(descriptor.Resolve(first.ServiceProvider));
        var secondHandler = Assert.IsType<TestHandler>(descriptor.Resolve(second.ServiceProvider));
        Assert.Same(first.ServiceProvider.GetRequiredService<Marker>(), firstHandler.Marker);
        Assert.Same(first.ServiceProvider.GetRequiredService<TestDb>(), firstHandler.Db);
        Assert.NotSame(firstHandler.Marker, secondHandler.Marker);
        Assert.NotSame(firstHandler.Db, secondHandler.Db);
        using var message = JsonDocument.Parse("{}");
        Assert.Equal(
            InboxResult.Applied,
            await firstHandler.HandleAsync(firstHandler.Db, message.RootElement, CancellationToken.None)
        );
        Assert.Throws<InvalidOperationException>(() => descriptor.Resolve(provider));
    }

    [Fact]
    public async Task Existing_singleton_handler_is_rejected()
    {
        var services = CreateServices();
        services.AddSingleton<SingletonHandler>();
        services.AddOrderedInbox<TestDb>().AddHandler<SingletonHandler>("locations", h => Subscribe(h, "locations"));

        await using var provider = BuildProvider(services);
        Assert.Contains("not singleton", Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message);
    }

    [Fact]
    public async Task Missing_default_connection_is_rejected()
    {
        var services = new ServiceCollection();
        AddApplicationServices(services, addConnection: false);
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));

        await using var provider = BuildProvider(services);
        Assert.Contains(
            "default INatsConnection",
            Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message
        );
    }

    [Fact]
    public async Task Keyed_connection_is_supported_and_missing_key_is_rejected()
    {
        var services = CreateServices();
        services.AddKeyedSingleton<INatsConnection>("main", static (_, _) => new NatsConnection());
        services
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>(
                "locations",
                h => h.Subscribe("events", "EVENTS", "events.>", "locations", connectionName: "main")
            );
        await using var provider = BuildProvider(services);
        Assert.Equal("main", Assert.Single(Assert.Single(Catalog(provider).Handlers).Subscriptions).ConnectionName);

        var invalid = CreateServices();
        invalid
            .AddOrderedInbox<TestDb>()
            .AddHandler<TestHandler>(
                "locations",
                h => h.Subscribe("events", "EVENTS", "events.>", "locations", connectionName: "missing")
            );
        await using var invalidProvider = BuildProvider(invalid);
        Assert.Contains(
            "keyed 'missing'",
            Assert.Throws<OptionsValidationException>(() => Catalog(invalidProvider)).Message
        );
    }

    [Fact]
    public void Registration_does_not_open_connections_or_construct_business_handlers()
    {
        var builder = Host.CreateApplicationBuilder();
        AddApplicationServices(builder.Services, addConnection: false);
        builder.Services.AddSingleton<INatsConnection>(_ =>
            throw new InvalidOperationException("NATS factory must not run.")
        );
        builder.Services.AddScoped<SingletonHandler>(_ =>
            throw new InvalidOperationException("Handler factory must not run.")
        );
        builder
            .Services.AddOrderedInbox<TestDb>()
            .AddHandler<SingletonHandler>("locations", h => Subscribe(h, "locations"));

        using var host = builder.Build();
        Assert.Single(host.Services.GetRequiredService<InboxCatalog<TestDb>>().Handlers);
        Assert.Equal(5, host.Services.GetServices<IHostedService>().Count());
    }

    [Fact]
    public async Task Exception_classifier_inherits_defaults_and_handler_override_is_snapshotted()
    {
        var services = CreateServices();
        Func<Exception, InboxResult> fallback = static _ => InboxResult.Retry("Default retry");
        Func<Exception, InboxResult> permanent = static _ => InboxResult.Reject("Permanent error");
        InboxHandlerOptions? retained = null;
        services
            .AddOrderedInbox<TestDb>(o => o.Defaults.ClassifyException = fallback)
            .AddHandler<TestHandler>(
                "locations",
                h =>
                {
                    Subscribe(h, "locations");
                    retained = h;
                    h.ClassifyException = permanent;
                }
            )
            .AddHandler<TestHandler>("products", h => Subscribe(h, "products"));
        await using var provider = BuildProvider(services);
        var catalog = Catalog(provider);
        Assert.Same(permanent, catalog.Handlers[0].Processing.ClassifyException);
        Assert.Same(fallback, catalog.Handlers[1].Processing.ClassifyException);
        retained!.ClassifyException = fallback;
        Assert.Same(permanent, catalog.Handlers[0].Processing.ClassifyException);
    }

    [Fact]
    public async Task Singleton_diagnostics_registration_is_rejected_before_processing()
    {
        var services = CreateServices();
        services.AddSingleton<InboxDiagnostics>();
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));
        await using var provider = BuildProvider(services);
        var error = Assert.Throws<OptionsValidationException>(() => Catalog(provider));
        Assert.Contains("InboxDiagnostics must be registered as scoped", error.Message);
    }

    [Fact]
    public async Task Snapshot_is_not_changed_by_later_options_or_configuration_mutations()
    {
        var services = CreateServices();
        InboxHandlerOptions? retained = null;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection().Build();
        services
            .AddOrderedInbox<TestDb>(configuration)
            .AddHandler<TestHandler>(
                "locations",
                h =>
                {
                    retained = h;
                    Subscribe(h, "locations");
                }
            );

        await using var provider = BuildProvider(services);
        var catalog = Catalog(provider);
        retained!.Retry.MaxAttempts = 99;
        retained.Subscriptions["locations-events"].Subject = "modified.>";
        configuration["Defaults:MaxParallelObjects"] = "99";
        configuration.Reload();
        Assert.Equal(5, catalog.Handlers[0].Processing.Retry.MaxAttempts);
        Assert.Equal(4, catalog.Handlers[0].Processing.MaxParallelObjects);
        Assert.Equal("events.locations.>", catalog.Handlers[0].Subscriptions[0].Subject);
    }

    [Theory]
    [InlineData("Schema", "public;drop schema public")]
    [InlineData("Schema", "pg_internal")]
    [InlineData("Defaults:MaxParallelObjects", "0")]
    [InlineData("Defaults:HandlerTimeout", "00:00:00")]
    [InlineData("Defaults:GapTimeout", "-00:00:01")]
    [InlineData("Defaults:PollInterval", "00:00:00.000001")]
    [InlineData("Defaults:Retry:MaxAttempts", "0")]
    [InlineData("Defaults:Retry:InitialDelay", "-00:00:01")]
    [InlineData("Defaults:Retry:MaxDelay", "00:00:00.500")]
    [InlineData("Defaults:IntakeConcurrency", "0")]
    [InlineData("Handlers:locations:Subscriptions:locations-events:BatchSize", "0")]
    [InlineData("Handlers:locations:Subscriptions:locations-events:DurableName", "location-service.locations")]
    public async Task Invalid_settings_are_rejected(string key, string value)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["Handlers:locations:Subscriptions:locations-events:Stream"] = "EVENTS",
                    ["Handlers:locations:Subscriptions:locations-events:Subject"] = "events.>",
                    ["Handlers:locations:Subscriptions:locations-events:DurableName"] = "locations",
                    [key] = value,
                }
            )
            .Build();
        var services = CreateServices();
        services.AddOrderedInbox<TestDb>(configuration).AddHandler<TestHandler>("locations");

        await using var provider = BuildProvider(services);
        Assert.Throws<OptionsValidationException>(() => Catalog(provider));
    }

    [Fact]
    public async Task Subscription_cannot_override_handler_processing_policy()
    {
        var services = CreateServices();
        services
            .AddOrderedInbox<TestDb>(
                JsonConfiguration(
                    """
                    {
                      "Handlers": { "locations": { "Subscriptions": {
                        "events": {
                            "Stream": "EVENTS", "Subject": "events.>",
                            "DurableName": "locations", "DlqPolicy": "BlockStream"
                        }
                      } } }
                    }
                    """
                )
            )
            .AddHandler<TestHandler>("locations");

        await using var provider = BuildProvider(services);
        Assert.Contains("DlqPolicy", Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message);
    }

    [Fact]
    public async Task Enabled_handler_requires_a_subscription()
    {
        var services = CreateServices();
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations");
        await using var provider = BuildProvider(services);
        Assert.Contains(
            "at least one subscription",
            Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message
        );
    }

    [Fact]
    public async Task Application_context_cannot_be_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton<TestDb>(_ =>
            new(new DbContextOptionsBuilder<TestDb>().UseNpgsql("Host=localhost").Options)
        );
        services.AddScoped<Marker>();
        services.AddSingleton<INatsConnection>(static _ => new NatsConnection());
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));

        await using var provider = BuildProvider(services);
        Assert.Contains(
            "DbContext must be registered as scoped",
            Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message
        );
    }

    [Fact]
    public async Task Connection_cannot_be_scoped()
    {
        var services = new ServiceCollection();
        AddApplicationServices(services, addConnection: false);
        services.AddScoped<INatsConnection>(static _ => new NatsConnection());
        services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));

        await using var provider = BuildProvider(services);
        Assert.Contains(
            "INatsConnection must be singleton",
            Assert.Throws<OptionsValidationException>(() => Catalog(provider)).Message
        );
    }

    [Fact]
    public async Task Unconfigured_application_context_prevents_other_hosted_services_from_starting()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestDb>();
        builder.Services.AddScoped<Marker>();
        builder.Services.AddSingleton<INatsConnection>(static _ => new NatsConnection());
        var observer = new StartupObserver();
        builder.Services.AddSingleton<IHostedService>(observer);
        builder.Services.AddOrderedInbox<TestDb>().AddHandler<TestHandler>("locations", h => Subscribe(h, "locations"));

        using var host = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.False(observer.Started);
    }

    [Fact]
    public async Task All_disabled_handlers_can_start_without_database_or_broker_registration()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddOrderedInbox<TestDb>().AddHandler<SingletonHandler>("locations", h => h.Enabled = false);
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    private static IServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        AddApplicationServices(services);
        return services;
    }

    private static void AddApplicationServices(IServiceCollection services, bool addConnection = true)
    {
        services.AddLogging();
        services.AddDbContext<TestDb>(o =>
            o.UseNpgsql("Host=localhost;Database=inbox_tests;Username=unused;Password=unused")
        );
        services.AddScoped<Marker>();
        if (addConnection)
            services.AddSingleton<INatsConnection>(static _ => new NatsConnection());
    }

    private static ServiceProvider BuildProvider(IServiceCollection services) =>
        services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });

    private static InboxCatalog<TestDb> Catalog(IServiceProvider provider) =>
        provider.GetRequiredService<InboxCatalog<TestDb>>();

    private static IConfiguration JsonConfiguration(string json)
    {
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder().AddJsonStream(stream).Build();
    }

    private static void Subscribe(InboxHandlerOptions options, string id) =>
        options.Subscribe($"{id}-events", "DOMAIN_EVENTS", $"events.{id}.>", $"example-{id}");

    private sealed class TestDb(DbContextOptions<TestDb> options) : DbContext(options);

    private sealed class Marker
    {
        public int Calls { get; set; }
    }

    private sealed class AbsentDependency;

    private sealed class TestHandler(TestDb db, Marker marker) : IInboxHandler<TestDb>
    {
        public TestDb Db => db;
        public Marker Marker => marker;

        public Task<InboxResult> HandleAsync(TestDb currentDb, JsonElement message, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Same(Db, currentDb);
            return Task.FromResult(InboxResult.Applied);
        }
    }

    private sealed class SingletonHandler : IInboxHandler<TestDb>
    {
        public Task<InboxResult> HandleAsync(TestDb db, JsonElement message, CancellationToken cancellationToken) =>
            Task.FromResult(InboxResult.Applied);
    }

    private sealed class StartupObserver : IHostedService
    {
        internal bool Started { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Started = true;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
