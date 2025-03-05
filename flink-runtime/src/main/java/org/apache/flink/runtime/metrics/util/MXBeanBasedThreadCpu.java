/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.util;

import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.util.SysInfoLinux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.management.HotspotThreadMBean;
import sun.management.ManagementFactoryHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Profiling cpu usage of each thread and classifying them by thread type. 3 way to profile: *
 * {@link ThreadMXBean#getThreadCpuTime(long)} to profile normal java user thread; * {@link
 * HotspotThreadMBean#getInternalThreadCpuTimes()} to profile internal thread in JVM, including GC
 * or so. * Leveraging proc based fs system to profile native threads, especially rocksdb async
 * threads.
 */
public class MXBeanBasedThreadCpu {
    private static final Logger LOG = LoggerFactory.getLogger(MXBeanBasedThreadCpu.class);

    /** Indicates the thread type. */
    public enum ThreadType {
        Task,
        GC,
        StateAsync,
        Others
    }

    private static final Pattern STATE_ASYNC_PATTERN =
            Pattern.compile(
                    "^.+_\\((\\d+)/(\\d+)\\)-"
                            + "((snapshot-\\d+)|(io-\\d+)|(GeminiReleaseManager-\\d+)"
                            + "|(page-\\d+)|(page-compaction-\\d+)|(TickTimeService-\\d+)"
                            + "|(GeminiMemoryGenerationControl)|(GeminiChunkCompactor-\\d+)"
                            + "|(GeminiChunkScheduler-\\d+)"
                            + "|(Flink-RocksDBStateDataTransfer-thread-\\d+)|(Gemini-embedded-StateDataTransfer-\\d+))$");

    private static final Pattern ROCKSDB_ASYNC_PATTERN = Pattern.compile("^rocksdb:.+$");

    // Ref: https://blog.csdn.net/zhangyingchengqi/article/details/119294886
    private static final Pattern GC_PATTERN =
            Pattern.compile(
                    "^((GC Daemon)|(Finalizer)|(Reference Handler)|(Gang worker#\\d+.*)"
                            + "|(GC task thread#\\d+.*)|(Concurrent Mark-Sweep GC Thread)|(.*Concurrent GC.*))$");

    private static final Pattern TASK_PATTERN = Pattern.compile("^.+\\((\\d+)/(\\d+)\\)#\\d+$");

    private final ThreadMXBean threadMxBean;
    private final HotspotThreadMBean hotspotThreadMBean;
    private final ProcfsBasedThreadTree procfsBasedThreadTree;

    /* used in java user thread */
    private final Map<Long, Long> threadInitialCPU;
    private final Map<Long, ThreadType> threadTypes;
    private final Map<ThreadType, Double> values;

    /* used in internal JVM thread */
    private final Map<String, Long> internalThreadInitialCPU;
    private final Map<String, ThreadType> internalThreadTypes;

    /* used in native thread */
    private final Set<String> knownRocksdbThreads;

    private long initialTime = Long.MAX_VALUE;

    MXBeanBasedThreadCpu() {
        threadMxBean = tryGetThreadMxBean();
        hotspotThreadMBean = tryGetHotspotThreadMBean();
        procfsBasedThreadTree = tryGetProcfsBasedThreadTree();
        threadInitialCPU = new HashMap<>();
        threadTypes = new HashMap<>();
        values = new HashMap<>();
        internalThreadInitialCPU = new HashMap<>();
        internalThreadTypes = new HashMap<>();
        knownRocksdbThreads = new HashSet<>();
    }

    private static ThreadMXBean tryGetThreadMxBean() {
        try {
            ThreadMXBean threadMxBean = ManagementFactoryHelper.getThreadMXBean();
            if (!threadMxBean.isThreadCpuTimeEnabled()) {
                threadMxBean.setThreadCpuTimeEnabled(true);
            }
            if (threadMxBean.isThreadCpuTimeEnabled()) {
                return threadMxBean;
            }
        } catch (Throwable e) {
        }
        return null;
    }

    private static HotspotThreadMBean tryGetHotspotThreadMBean() {
        try {
            return ManagementFactoryHelper.getHotspotThreadMBean();
        } catch (Throwable e) {
        }
        return null;
    }

    private static ProcfsBasedThreadTree tryGetProcfsBasedThreadTree() {
        try {
            return new ProcfsBasedThreadTree();
        } catch (Throwable e) {
        }
        return null;
    }

    public boolean isEnabled() {
        return threadMxBean != null;
    }

    public synchronized boolean isStateful() {
        return threadTypes.values().stream().anyMatch(e -> e == ThreadType.StateAsync);
    }

    public synchronized void registerCurrentThread(ThreadType type) {
        long threadId = Thread.currentThread().getId();
        threadTypes.put(threadId, type);
    }

    public synchronized double getValue(ThreadType type) {
        return values.getOrDefault(type, 0D);
    }

    /** Update cpu usage using 3 ways profiling. */
    synchronized void updateThreadCpuInGroup() {
        if (!isEnabled()) {
            return;
        }
        values.clear();
        long currentTime = System.nanoTime();
        long passedMills = currentTime - initialTime;

        updateBeanThread(passedMills);
        updateInternalHotpotThread(passedMills);
        updateProcfsBasedThread(passedMills);

        initialTime = currentTime;
    }

    /** Update java user thread info, classify then into different types. */
    synchronized void updateThreadInfo() {
        if (!isEnabled()) {
            return;
        }
        long[] threadIds = threadMxBean.getAllThreadIds();
        ThreadInfo[] threadInfo = threadMxBean.getThreadInfo(threadIds);

        Map<Long, ThreadType> currentThreadTypes = new HashMap<>(threadInfo.length);
        for (ThreadInfo info : threadInfo) {
            if (info == null) {
                continue;
            }
            ThreadType type = threadTypes.get(info.getThreadId());
            if (type == null) {
                type = determineThreadTypeFromName(info.getThreadName(), false);
                LOG.debug("Thread name '{}' is classified into {}.", info.getThreadName(), type);
            }
            currentThreadTypes.put(info.getThreadId(), type);
        }
        threadTypes.clear();
        threadTypes.putAll(currentThreadTypes);
    }

    /**
     * Update java user thread cpu usage leveraging {@link ThreadMXBean#getThreadCpuTime(long)}.
     *
     * @param passedMills the mills since last update.
     */
    private void updateBeanThread(long passedMills) {
        for (Map.Entry<Long, ThreadType> entry : threadTypes.entrySet()) {
            long threadId = entry.getKey();
            ThreadType type = entry.getValue();
            if (type == ThreadType.Others && procfsBasedThreadTree != null) {
                // We deduct other three parts from the process CPU usage to get the 'Others'.
                continue;
            }
            double cpuUsage;
            Long initialCpuTime = threadInitialCPU.get(threadId);
            long cpuTime = threadMxBean.getThreadCpuTime(threadId);
            if (cpuTime == -1L || initialCpuTime == null || passedMills <= 0) {
                cpuUsage = 0D;
            } else {
                long elapsedCpu = cpuTime - initialCpuTime;
                cpuUsage = elapsedCpu / (passedMills * 1D);
            }
            threadInitialCPU.put(threadId, cpuTime == -1L ? 0L : cpuTime);
            values.compute(type, (k, v) -> (v == null ? 0D : v) + cpuUsage);
        }
    }

    /**
     * Update internal JVM thread cpu usage leveraging {@link
     * HotspotThreadMBean#getInternalThreadCpuTimes()}.
     *
     * @param passedMills the mills since last update.
     */
    private void updateInternalHotpotThread(long passedMills) {
        if (hotspotThreadMBean != null) {
            Map<String, Long> internalThreadCpuTimes =
                    hotspotThreadMBean.getInternalThreadCpuTimes();
            for (Map.Entry<String, Long> entry : internalThreadCpuTimes.entrySet()) {
                ThreadType type = internalThreadTypes.get(entry.getKey());
                if (type == null) {
                    type = determineThreadTypeFromName(entry.getKey(), true);
                    LOG.debug(
                            "Internal thread name '{}' is classified into {}.",
                            entry.getKey(),
                            type);
                    internalThreadTypes.put(entry.getKey(), type);
                }
                if (type == ThreadType.Others && procfsBasedThreadTree != null) {
                    // We deduct other three parts from the process CPU usage to get the 'Others'.
                    continue;
                }
                Long initialCpuTime = internalThreadInitialCPU.get(entry.getKey());
                double cpuUsage;
                if (entry.getValue() == -1L || initialCpuTime == null || passedMills <= 0) {
                    cpuUsage = 0D;
                } else {
                    long elapsedCpu = entry.getValue() - initialCpuTime;
                    cpuUsage = elapsedCpu / (passedMills * 1D);
                }
                internalThreadInitialCPU.put(
                        entry.getKey(), entry.getValue() == -1L ? 0L : entry.getValue());
                values.compute(type, (k, v) -> (v == null ? 0D : v) + cpuUsage);
            }
        }
    }

    /**
     * Update native thread cpu usage using proc files.
     *
     * @param passedMills the mills since last update.
     */
    void updateProcfsBasedThread(long passedMills) {
        if (procfsBasedThreadTree != null) {
            procfsBasedThreadTree.updateProcessTree();
            procfsBasedThreadTree.updateSelfProcess();
            Map<String, ProcfsBasedThreadTree.ProcessInfo> processInfoMap =
                    procfsBasedThreadTree.getProcessTree();
            for (Map.Entry<String, ProcfsBasedThreadTree.ProcessInfo> entry :
                    processInfoMap.entrySet()) {
                String pid = entry.getValue().getPid();
                if (!knownRocksdbThreads.contains(pid)) {
                    if (ROCKSDB_ASYNC_PATTERN.matcher(entry.getValue().getName()).find()) {
                        knownRocksdbThreads.add(pid);
                        LOG.debug(
                                "Native thread name '{}' is classified into {}.",
                                entry.getValue().getName(),
                                ThreadType.StateAsync);
                    } else {
                        procfsBasedThreadTree.insertBlackList(pid);
                        continue;
                    }
                }

                long cpuTime = entry.getValue().getCpuTime();
                double cpuUsage;
                if (cpuTime == -1L || passedMills <= 0) {
                    cpuUsage = 0D;
                } else {
                    cpuUsage = cpuTime * 1000000D / passedMills;
                }
                values.compute(ThreadType.StateAsync, (k, v) -> (v == null ? 0D : v) + cpuUsage);
            }
            // we update 'Others' threads here
            ProcfsBasedThreadTree.ProcessInfo selfInfo = procfsBasedThreadTree.getSelfProcessInfo();
            if (selfInfo != null) {
                long cpuTime = selfInfo.getCpuTime();
                if (cpuTime != -1L && passedMills > 0) {
                    double cpuUsage = cpuTime * 1000000D / passedMills;
                    for (Map.Entry<ThreadType, Double> entry : values.entrySet()) {
                        if (entry.getKey() != ThreadType.Others) {
                            cpuUsage -= entry.getValue();
                        }
                    }
                    values.put(ThreadType.Others, cpuUsage > 0 ? cpuUsage : 0D);
                }
            }
        }
    }

    private static ThreadType determineThreadTypeFromName(String name, boolean internalThread) {
        if (!internalThread && STATE_ASYNC_PATTERN.matcher(name).find()) {
            return ThreadType.StateAsync;
        } else if (GC_PATTERN.matcher(name).find()) {
            return ThreadType.GC;
        } else if (!internalThread && TASK_PATTERN.matcher(name).find()) {
            // determine task last to exclude the state ones.
            return ThreadType.Task;
        }
        return ThreadType.Others;
    }

    /**
     * A Proc file-system based ProcessTree. Works only on Linux. Copied and modified from {@link
     * ProcfsBasedProcessTree}. Note that the 'process' indicates 'thread' in context of this class.
     */
    private static class ProcfsBasedThreadTree {

        private static final String SELF = "/proc/self";
        private static final String TASKS = "/proc/self/task";

        private static final Pattern PROCFS_STAT_FILE_FORMAT =
                Pattern.compile(
                        "^([\\d-]+)\\s\\((.*)\\)\\s[^\\s]\\s([\\d-]+)\\s([\\d-]+)\\s"
                                + "([\\d-]+)\\s([\\d-]+\\s){7}(\\d+)\\s(\\d+)\\s([\\d-]+\\s){7}(\\d+)\\s"
                                + "(\\d+)(\\s[\\d-]+){15}");

        private static final String PROCFS_STAT_FILE = "stat";

        private static final long JIFFY_LENGTH_IN_MILLIS =
                SysInfoLinux.JIFFY_LENGTH_IN_MILLIS; // in millisecond

        private static final Pattern numberPattern = Pattern.compile("[1-9][0-9]*");

        // to enable testing, using this variable which can be configured
        // to a test directory.
        private final String procfsDir;
        private final String selfDir;

        Map<String, ProcessInfo> processTree = new HashMap<>();
        ProcessInfo self;
        Set<String> updateBlackList = new HashSet<>();

        ProcfsBasedThreadTree() throws IOException {
            this(new File(SELF).getAbsolutePath(), new File(TASKS).getAbsolutePath());
        }

        ProcfsBasedThreadTree(String selfDir, String procfsDir) {
            this.selfDir = selfDir;
            this.procfsDir = procfsDir;
        }

        /** Get the list of all processes in the system. */
        private Set<String> getProcessList() {
            Set<String> processList = Collections.emptySet();
            FileFilter procListFileFilter =
                    new AndFileFilter(
                            DirectoryFileFilter.INSTANCE, new RegexFileFilter(numberPattern));

            File dir = new File(procfsDir);
            File[] processDirs = dir.listFiles(procListFileFilter);

            if (ArrayUtils.isNotEmpty(processDirs)) {
                processList = new HashSet<>(processDirs.length);
                for (File processDir : processDirs) {
                    processList.add(processDir.getName());
                }
            }
            return processList;
        }

        /**
         * Construct the ProcessInfo using the process' PID and procfs rooted at the specified
         * directory and return the same. It is provided mainly to assist testing purposes.
         *
         * <p>Returns null on failing to read from procfs,
         *
         * @param pinfo ProcessInfo that needs to be updated
         * @param procfsDir root of the proc file system
         * @return updated ProcessInfo, null on errors.
         */
        private static ProcessInfo constructProcessInfo(ProcessInfo pinfo, String procfsDir) {
            ProcessInfo ret = null;
            // Read "procfsDir/<pid>/stat" file - typically /proc/<pid>/stat
            BufferedReader in = null;
            InputStreamReader fReader = null;
            try {
                File pidDir = new File(procfsDir, pinfo.getPid());
                fReader =
                        new InputStreamReader(
                                new FileInputStream(new File(pidDir, PROCFS_STAT_FILE)),
                                Charset.forName("UTF-8"));
                in = new BufferedReader(fReader);
            } catch (FileNotFoundException f) {
                // The process vanished in the interim!
                return ret;
            }

            ret = pinfo;
            try {
                String str = in.readLine(); // only one line
                Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(str);
                boolean mat = m.find();
                if (mat) {
                    // Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
                    pinfo.updateProcessInfo(
                            m.group(2), Long.parseLong(m.group(7)), new BigInteger(m.group(8)));
                } else {
                    ret = null;
                }
            } catch (IOException io) {
                ret = null;
            } finally {
                // Close the streams
                try {
                    fReader.close();
                    try {
                        in.close();
                    } catch (IOException i) {
                    }
                } catch (IOException i) {
                }
            }

            return ret;
        }

        /**
         * Update process-tree with latest state. If the root-process is not alive, tree will be
         * empty.
         */
        public void updateProcessTree() {
            // Get the list of processes
            Set<String> processList = getProcessList();

            // cache the processTree to get the age for processes
            Map<String, ProcessInfo> oldProcs = new HashMap<String, ProcessInfo>(processTree);
            processTree.clear();

            int skipped = 0;
            for (String proc : processList) {
                // Get information for each process
                if (updateBlackList.contains(proc)) {
                    skipped++;
                } else {
                    ProcessInfo pInfo = new ProcessInfo(proc);
                    if (constructProcessInfo(pInfo, procfsDir) != null) {
                        processTree.put(proc, pInfo);
                    }
                }
            }

            // update age values and compute the number of jiffies since last update
            for (Map.Entry<String, ProcessInfo> procs : processTree.entrySet()) {
                ProcessInfo oldInfo = oldProcs.get(procs.getKey());
                if (procs.getValue() != null) {
                    procs.getValue().updateJiffy(oldInfo);
                }
            }

            if (skipped != updateBlackList.size()) {
                cleanupBlackList(processList);
            }
        }

        public void updateSelfProcess() {
            ProcessInfo pInfo = new ProcessInfo("");
            if (constructProcessInfo(pInfo, selfDir) != null) {
                if (self != null) {
                    pInfo.updateJiffy(self);
                }
                self = pInfo;
            }
        }

        public void insertBlackList(String pid) {
            updateBlackList.add(pid);
        }

        private void cleanupBlackList(Collection<String> validPids) {
            updateBlackList.removeIf(e -> !validPids.contains(e));
        }

        public Map<String, ProcessInfo> getProcessTree() {
            return processTree;
        }

        public ProcessInfo getSelfProcessInfo() {
            return self;
        }

        /** Class containing information of a process. */
        private static class ProcessInfo {
            private static final long UNAVAILABLE = -1;
            private String pid; // process-id
            private String name; // command name
            private Long utime = 0L; // # of jiffies in user mode
            private static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
            private BigInteger stime = new BigInteger("0"); // # of jiffies in kernel mode

            // # of jiffies used since last update:
            private Long dtime = 0L;

            // dtime = (utime + stime) - (utimeOld + stimeOld)
            // We need this to compute the cumulative CPU time
            // because the subprocess may finish earlier than root process

            public ProcessInfo(String pid) {
                this.pid = pid;
            }

            public String getPid() {
                return pid;
            }

            public String getName() {
                return name;
            }

            public Long getUtime() {
                return utime;
            }

            public BigInteger getStime() {
                return stime;
            }

            public Long getDtime() {
                return dtime;
            }

            public void updateProcessInfo(String name, Long utime, BigInteger stime) {
                this.name = name;
                this.utime = utime;
                this.stime = stime;
            }

            public void updateJiffy(ProcessInfo oldInfo) {
                if (oldInfo == null) {
                    BigInteger sum = this.stime.add(BigInteger.valueOf(this.utime));
                    if (sum.compareTo(MAX_LONG) > 0) {
                        this.dtime = 0L;
                    } else {
                        this.dtime = sum.longValue();
                    }
                    return;
                }
                this.dtime =
                        (this.utime
                                - oldInfo.utime
                                + this.stime.subtract(oldInfo.stime).longValue());
            }

            public long getCpuTime() {
                if (JIFFY_LENGTH_IN_MILLIS < 0) {
                    return UNAVAILABLE;
                }
                long incJiffies = getDtime();
                return incJiffies == UNAVAILABLE
                        ? UNAVAILABLE
                        : (incJiffies * JIFFY_LENGTH_IN_MILLIS);
            }

            @Override
            public String toString() {
                return "ProcessInfo{pid="
                        + pid
                        + ", name="
                        + name
                        + ", utime="
                        + utime
                        + ", stime="
                        + stime
                        + ", dtime="
                        + dtime
                        + "}";
            }
        }
    }
}
