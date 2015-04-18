package com.java.weedfs;

import com.java.util.InputStreamLoggerTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by lijc on 15/4/18.
 */
public class WeedFsMaster {
    private static final Logger log = LoggerFactory.getLogger(WeedFsMaster.class);

    @Autowired
    Environment env;

    private Process masterProcess;

    private InputStreamLoggerTask loggerTask;

    @PostConstruct
    public void init() {
        if (env.getProperty("blobstore.weedfs.master.enabled") != null &&
                !Boolean.parseBoolean(env.getProperty("blobstore.weedfs.master.enabled"))) {
            // no weedfs master node is needed
            return;
        }
        /* check if the master dir exists and create if neccessary */
        final File dir = new File(env.getProperty("blobstore.weedfs.master.dir"));
        if (!dir.exists()) {
            log.info("creating WeedFS master directory at " + dir.getAbsolutePath());
            if (!dir.mkdir()) {
                throw new IllegalArgumentException(
                        "Unable to create master directory. Please check the configuration");
            }
        }
        if (!dir.canRead() || !dir.canWrite()) {
            log.error("Unable to create master directory. The application was not initialiazed correctly");
            throw new IllegalArgumentException("Unable to use master directory. Please check the configuration");
        }
        if (env.getProperty("blobstore.weedfs.binary") == null) {
            throw new IllegalArgumentException("The WeedFs Binary path has to be set");
        }
        final File binary = new File(env.getProperty("blobstore.weedfs.binary"));
        if (!binary.exists()) {
            throw new IllegalArgumentException(new FileSystemNotFoundException(
                    "The weedfs binary can not be found at " + binary.getAbsolutePath()));
        }
        if (!binary.canExecute()) {
            throw new IllegalArgumentException("The weedfs binary at " + binary.getAbsolutePath() +
                    " can not be executed");
        }
        try {

            List<String> command = new ArrayList<String>(){{
                add(env.getProperty("blobstore.weedfs.binary"));
                add("master");
                add("-mdir=" + env.getProperty("blobstore.weedfs.master.dir"));
                add("-port=" + env.getProperty("blobstore.weedfs.master.port"));
                add("-ip=" + env.getProperty("blobstore.weedfs.master.public"));
            }};
            if (StringUtils.isNotBlank(env.getProperty("blobstore.weedfs.master.peers"))) {
                command.add("-peers=" + env.getProperty("blobstore.weedfs.master.peers"));
            }
            log.info("Starting weedfs master with command '" + String.join(" ", command) + "'");
            masterProcess = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectInput(ProcessBuilder.Redirect.PIPE)
                    .start();

            final Executor executor = Executors.newSingleThreadExecutor();
            if (!masterProcess.isAlive()) {
                throw new IOException("WeedFS Master could not be started! Exitcode " + masterProcess.exitValue());
            } else {
                log.info("WeedFs master is running");
                executor.execute(new InputStreamLoggerTask(masterProcess.getInputStream()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
