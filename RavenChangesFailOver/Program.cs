using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;

namespace RavenChangesFailOver
{
	/// <summary>
	///		Demonstration of how RavenDB's Changes API interacts with replication and fail-over.
	/// </summary>
	static class Program
	{
		/// <summary>
		///		The name of the replicated database to use.
		/// </summary>
		static readonly string					DatabaseName = "FailOverTest";

		/// <summary>
		///		The URL of the primary RavenDB server.
		/// </summary>
		static readonly string					PrimaryServerUrl = "http://localhost:8080/";

		/// <summary>
		///		The URLs of the secondary RavenDB servers.
		/// </summary>
		/// <remarks>
		///		Note that you will need at least 2 fail-over servers for this to work correctly when one of them goes down; probably because otherwise the system lacks a quorum.
		/// </remarks>
		static readonly IReadOnlyList<string>	SecondaryServerUrls = new string[]
		{
			"http://localhost:8081/",
			"http://localhost:8082/"
		};

		/// <summary>
		///		The Id of a document to load when polling the database to trigger fail-over.
		/// </summary>
		public static readonly string			TestDocumentId = "TestDocuments/1";

		/// <summary>
		///		RavenDB change notification APIs.
		/// </summary>
		static readonly List<ChangesApi>		_changesApis = new List<ChangesApi>();

		/// <summary>
		///		The subject used to aggregate connection events for change notification APIs.
		/// </summary>
		static readonly Subject<ChangesApi>		_apiConnectionSubject = new Subject<ChangesApi>(); 

		/// <summary>
		///		The main program entry-point.
		/// </summary>
		static void Main()
		{
			SynchronizationContext.SetSynchronizationContext(
				new SynchronizationContext()
			);

			ConfigureLogging();
			try
			{
				Log.Information("Initialising...");

				// Monitor server connection status.
				// If the active server is disconnected, try to find another available server to connect to.
				MonitorChangesApis();

				// The document store for the primary server.
				using (DocumentStore primaryDocumentStore = CreatePrimaryDocumentStore())
				using (CompositeDisposable secondaryStoreDisposal = new CompositeDisposable())
				{
					// Connect to the changes API for the primary server.
					ListenForChanges(primaryDocumentStore,
						changesApi => SubscribeToChangesApi(changesApi, primaryDocumentStore.Url)
					);

					// Connect changes APIs for fail-over (secondary) servers.
					foreach (ReplicationDestination failoverDestination in primaryDocumentStore.FailoverServers.ForDefaultDatabase)
					{
						if (failoverDestination.Url == primaryDocumentStore.Url)
							continue;

						Log.Verbose("Will use server '{FailoverServerUrl}' for fail-over.",
							failoverDestination.Url
						);

						// The document store for a fail-over (secondary) server.
						DocumentStore failoverDocumentStore = CreateSecondaryDocumentStore(
							failoverDestination.Url,
							failoverDestination.Database
						);
						secondaryStoreDisposal.Add(failoverDocumentStore);

						// Connect to the changes API for a secondary primary server.
						ListenForChanges(failoverDocumentStore,
							changesApi => SubscribeToChangesApi(changesApi, failoverDestination.Url)
						);
					}

					// Show us when we are failing over to another server.
					primaryDocumentStore
						.GetReplicationInformerForDatabase(primaryDocumentStore.DefaultDatabase)
						.FailoverStatusChanged += (sender, args) =>
						{
							Log.Verbose("Fail-over status changed ({ServerUrl} is now {ServerStatus}).",
								args.Url,
								args.Failing ? "Down" : "Up"
							);
						};

					using (PollDatabaseForFailOver(primaryDocumentStore, TestDocumentId))
					{
						Log.Information("Running (press enter to terminate).");
						Console.ReadLine();
					}
				}	
			}
			catch (Exception eUnexpected)
			{
				Log.Error(eUnexpected, "Unexpected error: {ErrorMessage}", eUnexpected.Message);
			}
		}

		/// <summary>
		///		Monitor connection status for change-notification APIs.
		/// </summary>
		static void MonitorChangesApis()
		{
			_apiConnectionSubject.Subscribe(connectionChanged =>
			{
				Log.Information("[{ServerUrl}] connection status changed: {ConnectionStatus}",
					connectionChanged.ServerUrl,
					connectionChanged.Api.Connected ? "Connected" : "Disconnected"
				);

				TrySubscribeToChanges();
			});
		}
		
		/// <summary>
		///		Attempt to find and subscribe to a Connected change-notification API.
		/// </summary>
		static void TrySubscribeToChanges()
		{
			// Walk through servers in descending order of preference until we find one we can subscribe to.
			
			bool subscribed = false;
			foreach (ChangesApi changesApi in _changesApis)
			{
				if (changesApi.IsConnected)
				{
					// We only want to subscribe to the first connected API.
					if (!subscribed)
					{
						changesApi.Subscribe();
						subscribed = true;

						continue;
					}
				}

				// All other change-notification APIs should be unsubscribed.
				changesApi.Unsubscribe();
			}

			if (!subscribed)
				Log.Warning("No servers are currently available; change notifications are disabled.");
		}

		/// <summary>
		///		An example subscription to the change-notification API.
		/// </summary>
		/// <param name="changes">
		///		The change-notification API.
		/// </param>
		/// <param name="serverUrl">
		///		The URL of the server targeted by the API.
		/// </param>
		/// <returns>
		///		An <see cref="IDisposable"/> representing the subscription.
		/// </returns>
		static IDisposable SubscribeToChangesApi(IDatabaseChanges changes, string serverUrl)
		{
			if (changes == null)
				throw new ArgumentNullException(nameof(changes));

			if (String.IsNullOrWhiteSpace(serverUrl))
				throw new ArgumentException("Argument cannot be null, empty, or composed entirely of whitespace: 'serverUrl'.", nameof(serverUrl));

			return changes.ForAllDocuments()
				.Where(
					change => !change.Id.StartsWith("Raven/Replication")
				)
				.Subscribe(
					change =>	Log.Information("{ServerUrl} {ChangeType}: {DocumentId} ({DocumentETag})", serverUrl, change.Type, change.Id, change.Etag),
					error =>	Log.Error(error, "{ServerUrl} subscription error: {ErrorMessage}", serverUrl, error.Message),
					() =>		Log.Information("[{ServerUrl}] subscription complete", serverUrl)
				);
		}
		
		/// <summary>
		///		Listen for changes to documents (if any).
		/// </summary>
		/// <param name="documentStore">
		///		The RavenDB document store on which to listen for changes.
		/// </param>
		/// <param name="subscribeToChangesApi">
		///		A delegate that subscribes to the change-notification API, returning an <see cref="IDisposable"/> representing the subscription.
		/// </param>
		static void ListenForChanges(DocumentStore documentStore, Func<IDatabaseChanges, IDisposable> subscribeToChangesApi)
		{
			if (documentStore == null)
				throw new ArgumentNullException(nameof(documentStore));

			if (subscribeToChangesApi == null)
				throw new ArgumentNullException(nameof(subscribeToChangesApi));

			ChangesApi changesApi = new ChangesApi(
				serverUrl: documentStore.Url,
				databaseName: documentStore.DefaultDatabase,
				api: documentStore.Changes(documentStore.DefaultDatabase),
				subscribe: subscribeToChangesApi
			);
			
			// The API connection subject aggregates connection status events from all servers.
			changesApi.ConnectionObservable.Subscribe(_apiConnectionSubject);

			_changesApis.Add(changesApi);
		}

		/// <summary>
		///		Create the RavenDB document store for the primary server.
		/// </summary>
		/// <returns>
		///		The configured document store.
		/// </returns>
		static DocumentStore CreatePrimaryDocumentStore()
		{
			DocumentStore documentStore = new DocumentStore
			{
				Url = PrimaryServerUrl,
				DefaultDatabase = DatabaseName,
				FailoverServers = new FailoverServers
				{
					ForDefaultDatabase = 
						SecondaryServerUrls.Select(serverUrl => new ReplicationDestination
						{
							Url = serverUrl,
							Database = DatabaseName
						})
						.ToArray()
				}
			};
			documentStore.Initialize();

			return documentStore;
		}

		/// <summary>
		///		Create the RavenDB document store for a secondary server.
		/// </summary>
		/// <returns>
		///		The configured document store.
		/// </returns>
		static DocumentStore CreateSecondaryDocumentStore(string serverUrl, string databaseName)
		{
			if (String.IsNullOrWhiteSpace(serverUrl))
				throw new ArgumentException("Argument cannot be null, empty, or composed entirely of whitespace: 'serverUrl'.", nameof(serverUrl));

			if (String.IsNullOrWhiteSpace(databaseName))
				throw new ArgumentException("Argument cannot be null, empty, or composed entirely of whitespace: 'databaseName'.", nameof(databaseName));

			DocumentStore documentStore = new DocumentStore
			{
				Url = serverUrl,
				DefaultDatabase = databaseName
			};
			documentStore.Initialize();

			return documentStore;
		}

		/// <summary>
		///		Periodically poll the database to trigger fail-over (if appropriate).
		/// </summary>
		/// <param name="primaryDocumentStore">
		///		The primary document store.
		/// </param>
		/// <param name="documentId">
		///		The Id of the document to load when polling the database.
		/// </param>
		/// <returns>
		///		An <see cref="IDisposable"/> that, when disposed, stops the polling.
		/// </returns>
		static IDisposable PollDatabaseForFailOver(DocumentStore primaryDocumentStore, string documentId)
		{
			if (primaryDocumentStore == null)
				throw new ArgumentNullException(nameof(primaryDocumentStore));

			if (String.IsNullOrWhiteSpace(documentId))
				throw new ArgumentException("Argument cannot be null, empty, or composed entirely of whitespace: 'documentId'.", nameof(documentId));

			// Periodically load a well-known document from the database to trigger replication fail-over (if required).
			return Observable.Interval(TimeSpan.FromSeconds(5)).Subscribe(_ =>
			{
				try
				{
					using (IDocumentSession session = primaryDocumentStore.OpenSession())
					{
						RavenJObject document = session.Load<RavenJObject>(documentId);
						if (document != null)
						{
							Log.Information("{DocumentId} = {DocumentJson}",
								documentId,
								document.ToString(Formatting.None)
							);
						}
						else
							Log.Warning("{DocumentId} is missing", documentId);
					}
				}
				catch (Exception eLoadDocument)
				{
					Log.Error(eLoadDocument, "Unable to load {DocumentId}: {ErrorMessage}", documentId, eLoadDocument.Message);
				}
			});
		}

		/// <summary>
		///		Configure logging.
		/// </summary>
		static void ConfigureLogging()
		{
			const string simpleLogOutputTemplate = "{Level}: {Message}{NewLine}{Exception}";

			Log.Logger =
				new LoggerConfiguration()
					.MinimumLevel.Verbose()
					.WriteTo.ColoredConsole(outputTemplate: simpleLogOutputTemplate)
					.WriteTo.Trace(outputTemplate: simpleLogOutputTemplate)
					.CreateLogger();
		}
	}
}
