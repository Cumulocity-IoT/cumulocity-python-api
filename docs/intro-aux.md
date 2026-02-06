The **Cumulocity Python API**'s standard packages (see
[Main API classes](docs-main.md) and
[Model classes](docs-model.md)) represent what the
Cumulocity REST API provides. The `c8y_tk` (for "Cumulocity
toolkit") module provides additional auxiliary tools that cover useful
functionality beyond the REST API but applicable in many projects.

The **`c8y_tk.notification2`** module provides listener
implementations that allow straightforward development of Notification
2.0 applications without additional overhead.

[→ Listener][c8y_tk.notification2.Listener]<br>
[→ QueueListener][c8y_tk.notification2.QueueListener]<br>
[→ AsyncListener][c8y_tk.notification2.AsyncListener]<br>
[→ AsyncQueueListener][c8y_tk.notification2.AsyncQueueListener]<br>

The **`c8y_tk.analytics`** module provides helper functions that
allow parallel query processing to maximise performance when dealing
with large datasets as well as easy transformation of a Cumulocity
Series to Pandas' data frames and series as well as NumPy arrays.

[→ ParallelExecutor][c8y_tk.analytics.ParallelExecutor]<br>
[→ to_data_frame][c8y_tk.analytics.to_data_frame]<br>
[→ to_numpy][c8y_tk.analytics.to_numpy]<br>
[→ to_series][c8y_tk.analytics.to_series]<br>

The **`c8y_tk.app`** module provides auxiliary tools for
implementing both interactive and micro service applications.

[→ CumulocityApp][c8y_tk.app.CumulocityApp]<br>
[→ SubscriptionListener][c8y_tk.app.SubscriptionListener]<br>
