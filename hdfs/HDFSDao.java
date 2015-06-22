package hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

public class HDFSDao {
	private static final String HDFS = "hdfs://localhost:9000/";
	private String hdfsPath;
	private Configuration conf;

	public HDFSDao(Configuration conf) {
		this(HDFS, conf);
	}

	public HDFSDao(String hdfs, Configuration conf) {
		this.hdfsPath = hdfs;
		this.conf = conf;
	}

	public static JobConf config() {
		JobConf conf = new JobConf(HDFSDao.class);
		conf.setJobName("HdfsDAO");
		conf.addResource("/usr/local/hadoop/conf/hdfs-site.xml");
		conf.addResource("/usr/local/hadoop/conf/core-site.xml");
		conf.addResource("/usr/local/hadoop/conf/mapred-site.xml");
		return conf;
	}

	public void mkdirs(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create: " + folder);
		}
		fs.close();
	}

	public void rmr(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		fs.deleteOnExit(path);
		System.out.println("Delete: " + folder);
		fs.close();
	}

	public void ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		System.out
				.println("==========================================================");
		for (FileStatus f : list) {
			System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(),
					f.isDir(), f.getLen());
		}
		System.out
				.println("==========================================================");
		fs.close();
	}

	public void createFile(String file, String content) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		byte[] buff = content.getBytes();
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(file));
			os.write(buff, 0, buff.length);
			System.out.println("Create: " + file);
		} finally {
			if (os != null)
				os.close();
		}
		fs.close();
	}

	public void copyFile(String local, String remote) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from: " + local + " to " + remote);
		fs.close();
	}

	public void download(String remote, String local) throws IOException {
		Path path = new Path(remote);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		fs.copyToLocalFile(path, new Path(local));
		System.out.println("download: from" + remote + " to " + local);
		fs.close();
	}

	public void cat(String remoteFile) throws IOException {
		Path path = new Path(remoteFile);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.setVerifyChecksum(false);
		FileSystem.getLocal(conf).setVerifyChecksum(false);
		FSDataInputStream fsdis = null;
		System.out.println("cat: " + remoteFile);
		try {
			fsdis = fs.open(path);
			IOUtils.copyBytes(fsdis, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(fsdis);
			fs.close();
		}
	}
}
