/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.app;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.apache.aurora.scheduler.config.CliOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MoreModulesTest {
  private static final String STRING = "string";

  @Test
  public void testInstantiate() {
    CliOptions options = new CliOptions();
    options.main.clusterName = "testing";
    Injector injector = Guice.createInjector(MoreModules.instantiateAll(
        ImmutableList.of(StringInstaller.class, StringSetInstaller.class),
        options));
    assertEquals(STRING, injector.getInstance(String.class));
    assertEquals(
        ImmutableSet.of(options.main.clusterName),
        injector.getInstance(Key.get(new TypeLiteral<Set<String>>() { })));
  }

  @Test
  public void testOptionsConstructorPreferred() {
    CliOptions options = new CliOptions();
    BothConstructors module = (BothConstructors) MoreModules.instantiate(
        BothConstructors.class, options);
    assertSame(options, module.options);
  }

  @Test
  public void testMissingConstructor() {
    try {
      MoreModules.instantiate(MissingConstructor.class, new CliOptions());
      fail("Expected missing constructor failure");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getCause() instanceof InstantiationException);
      assertEquals(
          "Failed to instantiate module " + MissingConstructor.class.getName()
              + ".Dynamic modules must have a default constructor or accept CliOptions",
          e.getMessage());
    }
  }

  @Test
  public void testInaccessibleConstructor() {
    try {
      MoreModules.instantiate(InaccessibleConstructor.class, new CliOptions());
      fail("Expected inaccessible constructor failure");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getCause() instanceof IllegalAccessException);
      assertEquals("Failed to instantiate module " + InaccessibleConstructor.class.getName()
          + ". Are you sure it's public?", e.getMessage());
    }
  }

  @Test
  public void testDefaultConstructorFailurePropagatesDirectly() {
    try {
      MoreModules.instantiate(ThrowingConstructor.class, new CliOptions());
      fail("Expected constructor failure");
    } catch (Exception e) {
      assertSame(ThrowingConstructor.FAILURE, e);
    }
  }

  @Test
  public void testOptionsConstructorFailureWrapped() {
    try {
      MoreModules.instantiate(ThrowingOptionsConstructor.class, new CliOptions());
      fail("Expected constructor failure");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getCause() instanceof InvocationTargetException);
      assertSame(ThrowingConstructor.FAILURE, e.getCause().getCause());
    }
  }

  @Test(expected = ClassCastException.class)
  public void testWrongType() {
    MoreModules.instantiate(Object.class, new CliOptions());
  }

  public static class BothConstructors extends AbstractModule {
    private final CliOptions options;

    public BothConstructors() {
      throw new AssertionError("Options constructor must be preferred");
    }

    public BothConstructors(CliOptions options) {
      this.options = options;
    }

    @Override
    protected void configure() {
      // No bindings.
    }
  }

  public static class MissingConstructor extends AbstractModule {
    public MissingConstructor(String ignored) {
      // Constructor intentionally has no side effects.
    }

    @Override
    protected void configure() {
      // No bindings.
    }
  }

  public static final class InaccessibleConstructor extends AbstractModule {
    private InaccessibleConstructor() { }

    @Override
    protected void configure() {
      // No bindings.
    }
  }

  public static class ThrowingConstructor extends AbstractModule {
    static final IOException FAILURE = new IOException("constructor failed");

    public ThrowingConstructor() throws IOException {
      throw FAILURE;
    }

    @Override
    protected void configure() {
      // No bindings.
    }
  }

  public static class ThrowingOptionsConstructor extends AbstractModule {
    public ThrowingOptionsConstructor(CliOptions options) throws IOException {
      throw ThrowingConstructor.FAILURE;
    }

    @Override
    protected void configure() {
      // No bindings.
    }
  }

  static class StringInstaller extends AbstractModule {
    @Override
    protected void configure() {
      bind(String.class).toInstance(STRING);
    }
  }

  public static class StringSetInstaller extends AbstractModule {
    private final CliOptions options;

    public StringSetInstaller(CliOptions options) {
      this.options = options;
    }

    @Override
    protected void configure() {
      bind(new TypeLiteral<Set<String>>() { })
          .toInstance(ImmutableSet.of(options.main.clusterName));
    }
  }
}
