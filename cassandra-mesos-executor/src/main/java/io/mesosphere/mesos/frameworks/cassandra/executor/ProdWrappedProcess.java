/**
 *    Copyright (C) 2015 Mesosphere, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

class ProdWrappedProcess implements WrappedProcess {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProdWrappedProcess.class);

    private final Process p;
    private final int pid;

    private static final Field pidField;

    static {
        try {
            Class cls = Class.forName("java.lang.UNIXProcess");
            pidField = cls.getDeclaredField("pid");
            pidField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    ProdWrappedProcess(Process p) {
        this.p = p;

        try {
            pid = pidField.getInt(p);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("not on Unix??", e);
        }
    }

    @Override
    public int getPid() {
        return pid;
    }

    @Override
    public void destroy() {
        LOGGER.info("Terminating process with PID {}", pid);
        p.destroy();
    }

    @Override
    public void destroyForcibly() {
        // note: with Java8 this can be a call to Process.destroyForcibly()

        LOGGER.info("Killing process with PID {}", pid);
        try {
            Process killProc = new ProcessBuilder("kill", "-9", Integer.toString(pid)).start();
            killProc.waitFor();
        } catch (Exception e) {
            LOGGER.error("Failed to forcibly terminate process with PID " + pid, e);
        }
    }

    @Override
    public int exitValue() {
        int exitCode = p.exitValue();
        return exitCode;
    }
}
