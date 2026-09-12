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
package org.apache.aurora.scheduler.http.api.security;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class Kerberos5RealmTest extends EasyMockTest {
  private static final AuthorizeHeaderToken TOKEN = new AuthorizeHeaderToken("Negotiate AQID");

  private GSSManager manager;
  private GSSCredential credential;
  private GSSContext context;
  private Kerberos5Realm realm;

  @Before
  public void setUp() {
    control.checkOrder(true);
    manager = createMock(GSSManager.class);
    credential = createMock(GSSCredential.class);
    context = createMock(GSSContext.class);
    realm = new Kerberos5Realm(manager, credential);
  }

  private void expectContext() throws GSSException {
    expect(manager.createContext(credential)).andReturn(context);
  }

  private void expectAccepted() throws GSSException {
    expectContext();
    expect(context.acceptSecContext(TOKEN.getAuthorizeHeaderValue(), 0, 3)).andReturn(null);
  }

  private void expectEstablished() throws GSSException {
    expectAccepted();
    expect(context.isEstablished()).andReturn(true);
    GSSName name = control.createMock("alice@EXAMPLE.COM", GSSName.class);
    expect(context.getSrcName()).andReturn(name);
  }

  private AuthenticationException authenticateFailure() {
    try {
      realm.getAuthenticationInfo(TOKEN);
      fail("Expected authentication failure");
      return null;
    } catch (AuthenticationException e) {
      return e;
    }
  }

  @Test
  public void testSuccessfulAuthenticationDisposesContext() throws Exception {
    expectEstablished();
    context.dispose();
    control.replay();

    AuthenticationInfo info = realm.getAuthenticationInfo(TOKEN);
    assertEquals("alice", info.getPrincipals().getPrimaryPrincipal());
    assertEquals(new KerberosPrincipal("alice@EXAMPLE.COM"),
        info.getPrincipals().oneByType(KerberosPrincipal.class));
    assertNull(info.getCredentials());
  }

  @Test
  public void testCreationFailureDoesNotDisposeSharedCredential() throws Exception {
    GSSException failure = new GSSException(GSSException.FAILURE);
    expect(manager.createContext(credential)).andThrow(failure);
    control.replay();

    assertSame(failure, authenticateFailure().getCause());
  }

  @Test
  public void testAcceptFailureDisposesContext() throws Exception {
    GSSException failure = new GSSException(GSSException.DEFECTIVE_TOKEN);
    expectContext();
    expect(context.acceptSecContext(TOKEN.getAuthorizeHeaderValue(), 0, 3)).andThrow(failure);
    context.dispose();
    control.replay();

    assertSame(failure, authenticateFailure().getCause());
  }

  @Test
  public void testIncompleteAuthenticationDisposesContext() throws Exception {
    expectAccepted();
    expect(context.isEstablished()).andReturn(false);
    context.dispose();
    control.replay();

    assertEquals("GSSContext was not established with a single message.",
        authenticateFailure().getMessage());
  }

  @Test
  public void testPrincipalFailureDisposesContext() throws Exception {
    GSSException failure = new GSSException(GSSException.BAD_NAME);
    expectAccepted();
    expect(context.isEstablished()).andReturn(true);
    expect(context.getSrcName()).andThrow(failure);
    context.dispose();
    control.replay();

    assertSame(failure, authenticateFailure().getCause());
  }

  @Test
  public void testUncheckedPrincipalFailureStillDisposesContext() throws Exception {
    IllegalArgumentException failure = new IllegalArgumentException("Invalid principal");
    expectAccepted();
    expect(context.isEstablished()).andReturn(true);
    expect(context.getSrcName()).andThrow(failure);
    context.dispose();
    control.replay();

    try {
      realm.getAuthenticationInfo(TOKEN);
      fail("Expected principal failure");
    } catch (IllegalArgumentException e) {
      assertSame(failure, e);
    }
  }

  @Test
  public void testDisposalFailureRejectsOtherwiseSuccessfulAuthentication() throws Exception {
    GSSException failure = new GSSException(GSSException.FAILURE);
    expectEstablished();
    context.dispose();
    expectLastCall().andThrow(failure);
    control.replay();

    assertSame(failure, authenticateFailure().getCause());
  }

  @Test
  public void testDisposalFailureIsSuppressedOnAuthenticationFailure() throws Exception {
    GSSException failure = new GSSException(GSSException.DEFECTIVE_TOKEN);
    GSSException disposalFailure = new GSSException(GSSException.FAILURE);
    expectContext();
    expect(context.acceptSecContext(TOKEN.getAuthorizeHeaderValue(), 0, 3)).andThrow(failure);
    context.dispose();
    expectLastCall().andThrow(disposalFailure);
    control.replay();

    AuthenticationException error = authenticateFailure();
    assertSame(failure, error.getCause());
    assertEquals(1, error.getSuppressed().length);
    assertSame(disposalFailure, error.getSuppressed()[0]);
  }

  @Test
  public void testUncheckedDisposalFailureIsSuppressedOnAuthenticationFailure() throws Exception {
    GSSException failure = new GSSException(GSSException.DEFECTIVE_TOKEN);
    IllegalStateException disposalFailure = new IllegalStateException("Disposal failed");
    expectContext();
    expect(context.acceptSecContext(TOKEN.getAuthorizeHeaderValue(), 0, 3)).andThrow(failure);
    context.dispose();
    expectLastCall().andThrow(disposalFailure);
    control.replay();

    AuthenticationException error = authenticateFailure();
    assertSame(failure, error.getCause());
    assertEquals(1, error.getSuppressed().length);
    assertSame(disposalFailure, error.getSuppressed()[0]);
  }

  @Test
  public void testDisposalErrorIsSuppressedOnAuthenticationFailure() throws Exception {
    GSSException failure = new GSSException(GSSException.DEFECTIVE_TOKEN);
    AssertionError disposalFailure = new AssertionError("Disposal failed");
    expectContext();
    expect(context.acceptSecContext(TOKEN.getAuthorizeHeaderValue(), 0, 3)).andThrow(failure);
    context.dispose();
    expectLastCall().andThrow(disposalFailure);
    control.replay();

    AuthenticationException error = authenticateFailure();
    assertSame(failure, error.getCause());
    assertEquals(1, error.getSuppressed().length);
    assertSame(disposalFailure, error.getSuppressed()[0]);
  }

  @Test
  public void testUncheckedDisposalFailureAfterSuccessIsPropagated() throws Exception {
    IllegalStateException failure = new IllegalStateException("Disposal failed");
    expectEstablished();
    context.dispose();
    expectLastCall().andThrow(failure);
    control.replay();

    try {
      realm.getAuthenticationInfo(TOKEN);
      fail("Expected disposal failure");
    } catch (IllegalStateException e) {
      assertSame(failure, e);
    }
  }

  @Test
  public void testDisposalErrorAfterSuccessIsPropagated() throws Exception {
    AssertionError failure = new AssertionError("Disposal failed");
    expectEstablished();
    context.dispose();
    expectLastCall().andThrow(failure);
    control.replay();

    try {
      realm.getAuthenticationInfo(TOKEN);
      fail("Expected disposal failure");
    } catch (AssertionError e) {
      assertSame(failure, e);
    }
  }

  @Test
  public void testDisposalDoesNotSuppressThePrimaryOntoItself() throws Exception {
    IllegalStateException failure = new IllegalStateException("Provider failed");
    expectContext();
    expect(context.acceptSecContext(TOKEN.getAuthorizeHeaderValue(), 0, 3)).andThrow(failure);
    context.dispose();
    expectLastCall().andThrow(failure);
    control.replay();

    try {
      realm.getAuthenticationInfo(TOKEN);
      fail("Expected provider failure");
    } catch (IllegalStateException e) {
      assertSame(failure, e);
      assertEquals(0, e.getSuppressed().length);
    }
  }
}
