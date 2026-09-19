# Foundation review reproduction

The parent compiled the seven current source files named below plus this synthetic probe with `.cache/java03-tools/java/jdk-25.0.4.1+1/bin/javac`, using `.cache/inplace-build/build/scheduler/install/aurora-scheduler/lib/*` as dependency classpath. Probe classes were emitted to `.cache/java25-review-20260918/probe-classes` and placed **before** the dependency jars on the execution classpath. This avoids relying on stale compiled helper classes. These are isolated helper checks; no cluster or external data is accessed.

Sources: commons MorePreconditions, Pair, Iterables2, Unit, Time, Amount and Credentials, at the hashes in foundation.jsonl.

```java
import java.util.*;
import org.apache.aurora.common.quantity.*;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.collections.Iterables2;
class FoundationProbe {
  public static void main(String[] args) {
    var a = Amount.of(1L, Time.DAYS);
    var b = Amount.of(24L, Time.HOURS);
    System.out.println("Amount equal="+a.equals(b)+" sameHash="+(a.hashCode()==b.hashCode())+" setSize="+new HashSet<>(List.of(a,b)).size());
    var c = new Credentials("digest", new byte[]{1,2,3});
    var d = new Credentials("digest", new byte[]{1,2,3});
    System.out.println("Credentials equal="+c.equals(d)+" sameHash="+(c.hashCode()==d.hashCode())+" setSize="+new HashSet<>(List.of(c,d)).size());
    var i = Iterables2.zip(0, List.of(1)).iterator();
    i.next();
    System.out.println("Zip exhausted="+!i.hasNext()+" extraNext="+i.next());
  }
}
```

Observed on the pinned Java 25 toolchain:

```text
Amount equal=true sameHash=false setSize=2
Credentials equal=true sameHash=false setSize=2
Zip exhausted=true extraNext=[0]
```

No production or test Java files were edited. Repeating this probe after a fix should assert equal hashes/set size 1 and expect NoSuchElementException from the exhausted iterator.
