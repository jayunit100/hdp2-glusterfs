/**
 *
 * Copyright (c) 2011 Red Hat, Inc. <http://www.redhat.com>
 * This file is part of GlusterFS.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Extends the RawLocalFileSystem to add support for Gluster Volumes. 
 * 
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterVolume extends RawLocalFileSystem{

    static final Logger log = LoggerFactory.getLogger(GlusterVolume.class);
    public static final URI NAME = URI.create("glusterfs:///");
    
    protected String root=null;
    
    protected static final GlusterFSXattr attr = new GlusterFSXattr();
    
    public GlusterVolume(){}
    
    public GlusterVolume(Configuration conf){
        super();
        this.setConf(conf);
    }
    public URI getUri() { return NAME; }
    
    public void setConf(Configuration conf){
        log.info("initializing gluster volume......  ....");
        super.setConf(conf);
        if(conf!=null){
         
            try{
                root=conf.get("fs.glusterfs.mount", null);
                //volName=conf.get("fs.glusterfs.volname", null);
                //remoteGFSServer=conf.get("fs.glusterfs.server", null);
                
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public Path getWorkingDirectory() {
        log.info("getWorkingDir = path : " + NAME.toString() );
        return new Path(NAME.toString() );
    }
    
    public File pathToFile(Path path) {
        log.info("pathToFile = path : " + path.toUri().getRawPath() );

        String pathString = path.toUri().getRawPath();
     
       // if(pathString.startsWith(Path.SEPARATOR)){
       //     pathString = pathString.substring(1);
       // }
        
        return new File(pathString);
    }
    
    public Path fileToPath(File path) {
        log.info("fileToPath " + path.toURI().getRawPath());
        /* remove the /mnt/glustersfs part of the file path */
        //String pathString = path.toURI().getRawPath().substring(root.length());
        //return new Path(path.toURI().getRawPath());
        return new Path(path.toURI().getRawPath());
    }
    
    public FileStatus[] listStatus(Path f) throws IOException {
        log.info("liststatus of " + f);

        File localf = pathToFile(f);
        FileStatus[] results;

        if (!localf.exists()) {
          throw new FileNotFoundException("File " + f + " does not exist");
        }
        if (localf.isFile() && !localf.isDirectory()) {
          return new FileStatus[] {
            new GlusterFileStatus(localf, getDefaultBlockSize(), this) };
        }

        File[] names = localf.listFiles();
        if (names == null) {
          return null;
        }
        results = new FileStatus[names.length];
        int j = 0;
        for (int i = 0; i < names.length; i++) {
            log.info(i+" loop");
            try {
            results[j++] = getFileStatus(new Path(names[i].getAbsolutePath()));
          } catch (FileNotFoundException e) {
              log.error("FNF EXception!!!" +names[i] + e.getMessage());
              // ignore the files not found since the dir list may have have changed
            // since the names[] list was generated.
          }
        }
        return results;
        /**
        if (j == names.length) {
          return results;
        }
        return Arrays.copyOf(results, j);
          **/
      }
    
    
    public FileStatus getFileStatus(Path f) throws IOException {
        log.info("getFileStatus " + f);

        File path = pathToFile(f);
        if (path.exists()) {
          return new GlusterFileStatus(pathToFile(f), getDefaultBlockSize(), this);
        } else {
          throw new FileNotFoundException( "File " + f + " does not exist.");
        }
      }

    
    public long getBlockSize(Path path) throws IOException{
        log.info("getFileStatus " + path);

        long blkSz;
        File f=pathToFile(path);

        blkSz=attr.getBlockSize(f.getPath());
        if(blkSz==0)
            blkSz=getLength(path);

        return blkSz;
    }
   
    public BlockLocation[] getFileBlockLocations(FileStatus file,long start,long len) throws IOException{
        log.info("getFileStatus " + file + " " +file.getPath());

        File f=pathToFile(file.getPath());
        BlockLocation[] result=null;

        result=attr.getPathInfo(f.getPath(), start, len);
        if(result==null){
            log.info("Problem getting destination host for file "+f.getPath());
            return null;
        }

        return result;
    }
    
    public String toString(){
        return "Gluster Volume mounted at: " + root;
    }

}
