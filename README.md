# jepsen.radix-dlt

Jepsen tests for the Radix distributed ledger system.

## Installation

In addition to a Jepsen environment, you'll need a RadixDLT client.

```
git clone https://github.com/radixdlt/radixdlt.git
git checkout release/1.0-beta.40
```

Compile and install each part of the client library. This apparently only builds with JDK 11, so you'll need to downgrade, compile, then flip back.

```
cd radixdlt-engine
echo "apply plugin: 'maven'" >> build.gradle
../gradlew install
```

```
cd ../radixdlt-java-common/
echo "apply plugin: 'maven'" >> build.gradle
../gradlew install
```

```
cd ../radixdlt-java/radixdlt-java
echo "apply plugin: 'maven'" >> build.gradle
../../gradlew install
```

## Usage

FIXME

## License

Copyright © 2021 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
