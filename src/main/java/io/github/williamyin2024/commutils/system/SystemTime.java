/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.williamyin2024.commutils.system;

import io.github.williamyin2024.commutils.time.Time;

import java.util.function.Supplier;

/**
 * A time implementation that uses the system clock and sleep call. Use `Time.SYSTEM` instead of creating an instance
 * of this class.
 */
public class SystemTime implements Time {

	@Override
	public long milliseconds() {
		return System.currentTimeMillis();
	}

	@Override
	public long nanoseconds() {
		return System.nanoTime();
	}

	@Override
	public void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			// this is okay, we just wake up early
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
		synchronized (obj) {
			while (true) {
				if (condition.get())
					return;

				long currentTimeMs = milliseconds();
				if (currentTimeMs >= deadlineMs)
					throw new InterruptedException("Condition not satisfied before deadline");

				obj.wait(deadlineMs - currentTimeMs);
			}
		}
	}

}
