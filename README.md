# jepsen.radix-dlt

A Clojure library designed to ... well, that part is up to you.

## Installation

In addition to a Jepsen environment, you'll need a RadixDLT client.

```
git clone https://github.com/radixdlt/radixdlt.git
cd radix-dlt/radixdlt-java/radixdlt-java
git checkout release/1.0-beta.35
echo "apply plugin: 'maven'" >> build.gradle
../../gradlew install
```

Likewise, radix-engine:

```
cd ../../radixdlt-engine
echo "apply plugin: 'maven'" >> build.gradle
../gradlew install
```

And radixdlt-java-common

```
cd ../radixdlt-java-common/
echo "apply plugin: 'maven'" >> build.gradle
../gradlew install
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
