package ru.apapikyan.learn.bigdata.yarn;

import java.io.BufferedInputStream;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DownloadFileService {

	private static final Log LOG = LogFactory.getLog(DownloadFileService.class);

	private String fileURL = null;
	// private String hdfsRootPath = null;

	private FileSystem fs = null;
	private Path outFile = null;
	private BufferedInputStream in = null;
	private FSDataOutputStream out = null;

	private int counter = -1;

	public DownloadFileService(String fileURL, String hdfsRootPath, String counter) throws Exception {
		this.fileURL = fileURL;
		// this.hdfsRootPath = hdfsRootPath;
		String p = DownloadUtilities.getFilePathFromURL(hdfsRootPath, this.fileURL);
		this.outFile = new Path(p);

		Configuration conf = new Configuration();
		this.fs = FileSystem.get(conf);

		try {
			this.counter = Integer.parseInt(counter);
		} catch (NumberFormatException nfe) {
			LOG.warn("Error pasing counter argument. Imput argument was [" + counter +"]");
		} finally {
			LOG.warn("Field counter equals " + this.counter);
		}
	}

	public void initializeDownload() throws Exception {

		LOG.warn("Trying to download... " + this.fileURL);

		this.in = new BufferedInputStream(new URL(this.fileURL).openStream());
		this.out = fs.create(outFile);
	}

	public void closeAll() throws Exception {
		if (in != null) {
			in.close();
		}
		if (out != null) {
			out.close();
		}
		if (fs != null) {
			fs.close();
		}
	}

	public void downloadAndSaveFileFromUrl() throws Exception {
		try {
			byte data[] = new byte[1024];
			int count;
			while ((count = in.read(data, 0, 1024)) != -1) {
				out.write(data, 0, count);
			}
		} finally {
			this.closeAll();
		}
	}

	public void performDownloadSteps() throws Exception {
		if (DownloadUtilities.doesFileExists(this.fs, this.outFile)) {
			LOG.warn(this.fileURL + " file is already downloaded");
			return;
		} else {
			this.initializeDownload();
			this.downloadAndSaveFileFromUrl();
		}
	}

	public static void main(String[] args) throws Exception {
		DownloadFileService service = null;
		try {
			String url = args[0].trim();
			String rootHDFSPath = args[1];
			String counter = args[2];

			LOG.warn("DFS arg[0] " + url);
			LOG.warn("DFS arg[1] " + rootHDFSPath);
			LOG.warn("DFS arg[2] " + counter);

			service = new DownloadFileService(url, rootHDFSPath, counter);
			service.performDownloadSteps();
		} finally {
			if (service != null)
				service.closeAll();
		}

	}
}