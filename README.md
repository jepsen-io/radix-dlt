# jepsen.radix-dlt

Jepsen tests for the Radix distributed ledger system.

## Installation

In addition to a Jepsen environment, you'll need a RadixDLT client. Which
client depends on which version of Radix you're trying to test--see project.clj
for several (commented-out) versions that might be appropriate. The most recent Radix build we're testing is:

```
feature/account-txn-log-write-read-consistency
```

And the client we're using with this build is:

```
git clone https://github.com/radixdlt/radixdlt.git
git checkout feature/client-support-for-txn-accounting
```

Compile and install each part of the client library. This apparently only
builds with JDK 11, so depending on your Gradle setup you might need to
downgrade, compile, then flip back.

```
cd radixdlt-engine
echo "apply plugin: 'maven'" >> build.gradle
../gradlew install

cd ../radixdlt-java-common/
echo "apply plugin: 'maven'" >> build.gradle
../gradlew install

cd ../radixdlt-java/radixdlt-java
echo "apply plugin: 'maven'" >> build.gradle
../../gradlew install
```

To see the exact version this installed, check:

```
ls ~/.m2/repository/com/radixdlt/radixdlt-java/
```

## Usage

```
lein run test-all --concurrency 3n
```

## Building Radix DLT

If you need to build a custom build of Radix to test a patch:

```sh
cd radix
git checkout <SOME-VERSION>
DOCKER_BUILDKIT=1 docker build --output type=local,dest=out --progress plain -f radixdlt-core/docker/Dockerfile.build .
```

This will spit out a zipfile like

```
out/distributions/radixdlt-1.0.0-<branch>-SNAPSHOT.zip
```

Which you can copy to any local path you like, then run

```sh
lein run test ... --zip path/to/radixdlt-1.0.0-whatever-SNAPSHOT.zip
```

## Testing Against Stokenet

To run tests against the public Stokenet, you'll need an address with XRD. Run
`lein run keygen` to construct a new account, and paste the results into
`stokenet.edn`; then fund that account with some XRD. Running tests with
`--stokenet` will use that account instead.

We don't have access to the raw txn APIs, so you'll need to run with

`--fs txn-log,balance`

## Passive Checks of Mainnet

This test suite also includes a passive checker which will perform read-only
queries against the Radix mainnet to identify two classes of consistency
anomalies: transactions which are present in one account's log but missing from
another, and transactions which are present in some log but in state `FAILED`.
To do this, check out the `1.0.0-compatible` branch, and run `lein pubcheck`:

```sh
git checkout 1.0.0-compatible
lein run pubcheck
```

This will take several hours. When it completes, run a second pass with

```sh
lein run pubcheck --recheck
```

... which will go back and find extra bugs. This checker isn't particularly
smart--it was a quick one-off and wasn't built to last.

## License

Copyright © 2021 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
