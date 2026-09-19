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
package org.apache.aurora.scheduler.http;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.aurora.common.stats.SlidingStats;

/**
 * An HTTP filter that exports counts and timing for requests based on response code.
 */
public class HttpStatsFilter extends AbstractFilter {

  private final LoadingCache<Integer, SlidingStats> counters = CacheBuilder.newBuilder()
      .build(new CacheLoader<Integer, SlidingStats>() {
        @Override
        public SlidingStats load(Integer status) {
          return new SlidingStats("http_" + status + "_responses", "nanos");
        }
      });

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    long start = System.nanoTime();
    chain.doFilter(request, response);
    counters.getUnchecked(response.getStatus()).accumulate(System.nanoTime() - start);
  }
}
