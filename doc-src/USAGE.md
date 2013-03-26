## Usage

The `server-spec` function provides a convenient pallet server spec for
cassandra.  It takes a single map as an argument, specifying configuration
choices, as described below for the `settings` function.  You can use this
in your own group or server specs in the `:extends` clause.

```clj
(require '[pallet.crate.cassandra :as cassandra])
(group-spec my-cassandra-group
  :extends [(cassandra/server-spec {})])
```

While `server-spec` provides an all-in-one function, you can use the individual
plan functions as you see fit.

The `settings` function provides a plan function that should be called in the
`:settings` phase.  The function puts the configuration options into the pallet
session, where they can be found by the other crate functions, or by other
crates wanting to interact with cassandra.

The `install` function is responsible for actually installing cassandra.

The `configure` function writes the cassandra configuration file, using the form
passed to the `:config` key in the `settings` function.
