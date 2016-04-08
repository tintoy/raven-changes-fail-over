using Raven.Client.Changes;
using Serilog;
using System;
using System.Reactive.Linq;

namespace RavenChangesFailOver
{
	/// <summary>
	///		Represents the change-notification API for a RavenDB server / database.
	/// </summary>
	public sealed class ChangesApi
	{
		/// <summary>
		///		An object used to synchronise access to state when subscribing / unsubscribing.
		/// </summary>
		/// <remarks>
		///		AF: This style of concurrency control is quick-and-dirty; since this is a demo, that's not really an issue but in a real (production-grade) applications there are better ways of implementing this.
		/// </remarks>
		readonly object									_stateLock = new object();

		/// <summary>
		///		A delegate that performs the subscription to the change-notification API.
		/// </summary>
		readonly Func<IDatabaseChanges, IDisposable>	_subscribe;

		/// <summary>
		///		Create a new <see cref="ChangesApi"/>.
		/// </summary>
		/// <param name="serverUrl">
		///		The RavenDB server URL.
		/// </param>
		/// <param name="databaseName">
		///		The RavenDB database name.
		/// </param>
		/// <param name="api">
		///		The change-notification API.
		/// </param>
		/// <param name="subscribe">
		///		A delegate that performs the subscription to the change-notification API.
		/// </param>
		public ChangesApi(string serverUrl, string databaseName, IDatabaseChanges api, Func<IDatabaseChanges, IDisposable> subscribe)
		{
			if (String.IsNullOrWhiteSpace(serverUrl))
				throw new ArgumentException("Argument cannot be null, empty, or composed entirely of whitespace: 'url'.", nameof(serverUrl));

			if (String.IsNullOrWhiteSpace(databaseName))
				throw new ArgumentException("Argument cannot be null, empty, or composed entirely of whitespace: 'databaseName'.", nameof(databaseName));

			if (api == null)
				throw new ArgumentNullException(nameof(api));

			ServerUrl = serverUrl;
			DatabaseName = databaseName;
			Api = api;
			_subscribe = subscribe;

			// Converts from what would be an IObservable<EventArgs> to an IObservable<ChangesApi>
			ConnectionObservable = Observable.FromEvent<EventHandler, ChangesApi>(
				conversion: publishEvent => (eventSender, eventArguments) => publishEvent(this),
				addHandler: handler => Api.ConnectionStatusChanged += handler,
				removeHandler: handler => Api.ConnectionStatusChanged -= handler
			);
		}

		/// <summary>
		///		The RavenDB server URL.
		/// </summary>
		public string ServerUrl { get; }

		/// <summary>
		///		The RavenDB database name.
		/// </summary>
		public string DatabaseName { get; }

		/// <summary>
		///		The RavenDB change-notification  API.
		/// </summary>
		public IDatabaseChanges Api { get; }

		/// <summary>
		///		Is the change-notification API currently connected to the server?
		/// </summary>
		public bool IsConnected => Api.Connected;

		/// <summary>
		///		The subscription (if any) to the change-notification API.
		/// </summary>
		public IDisposable Subscription { get; set; }

		/// <summary>
		///		Is there an active subscription to the change-notification API?
		/// </summary>
		public bool IsSubscribed => Subscription != null;

		/// <summary>
		///		The <see cref="IObservable{T}"/> used to publish events that indicate changes in the API's connection status.
		/// </summary>
		public IObservable<ChangesApi> ConnectionObservable { get; }

		/// <summary>
		///		Create a subscription to the change-notification API (if required).
		/// </summary>
		public void Subscribe()
		{
			lock (_stateLock)
			{
				if (IsSubscribed)
					return;

				Log.Verbose("[{ServerUrl}] will listen for changes.", ServerUrl);
				Subscription = _subscribe(Api);
			}
		}

		/// <summary>
		///		Destroy the subscription to the change notification API (if required).
		/// </summary>
		public void Unsubscribe()
		{
			lock (_stateLock)
			{
				if (!IsSubscribed)
					return;

				Log.Verbose("{ServerUrl} won't listen for changes.", ServerUrl);
				Subscription.Dispose();
				Subscription = null;
			}
		}
	}
}