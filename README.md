[Repository](https://github.com/pallet/cassandra-crate) &#xb7;
[Issues](https://github.com/pallet/cassandra-crate/issues) &#xb7;
[API docs](http://palletops.com/cassandra-crate/0.8/api) &#xb7;
[Annotated source](http://palletops.com/cassandra-crate/0.8/annotated/uberdoc.html) &#xb7;
[Release Notes](https://github.com/pallet/cassandra-crate/blob/develop/ReleaseNotes.md)

 A [pallet](http://palletops.com/) crate to install and configure
[cassandra](http://cassandra.apache.org/).

### Dependency Information

```clj
:dependencies [[com.palletops/cassandra-crate "0.8.0-alpha.1"]]
```

### Releases

<table>
<thead>
  <tr><th>Pallet</th><th>Crate Version</th><th>Repo</th><th>GroupId</th></tr>
</thead>
<tbody>
  <tr>
    <th>0.8.0-beta.5</th>
    <td>0.8.0-alpha.1</td>
    <td>clojars</td>
    <td>com.palletops</td>
    <td><a href='https://github.com/pallet/cassandra-crate/blob/0.8.0-alpha.1/ReleaseNotes.md'>Release Notes</a></td>
    <td><a href='https://github.com/pallet/cassandra-crate/blob/0.8.0-alpha.1/'>Source</a></td>
  </tr>
</tbody>
</table>

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

## Support

[On the group](http://groups.google.com/group/pallet-clj), or
[#pallet](http://webchat.freenode.net/?channels=#pallet) on freenode irc.

## License

Licensed under [EPL](http://www.eclipse.org/legal/epl-v10.html)

Copyright 2013 Hugo Duncan.
