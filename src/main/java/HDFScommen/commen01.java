package HDFScommen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.*;

public class commen01 {
    public static void main(String[] args) throws IOException {

        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();


        conf.set("fs.defaultFS","hdfs://192.168.244.100:8020");

        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(conf);


        //创建目录
        Boolean flag = fs.mkdirs(new Path("/test/0727"));
        System.out.println(flag);

        //判断文件是否存在
        Boolean isExists = fs.exists(new Path("/test/0727/1.txt"));
        System.out.println(isExists);
        if(isExists == false){
            fs.create(new Path("/test/0727/1.txt"));
        }



        //查看文件信息
   //     FileStatus fileStatus = fs.getFileStatus(new Path("/test/0727/1.txt"));
   ///     System.out.println(fileStatus.toString());

        //下载文件
      //  if(isExists == false){
      //      fs.copyToLocalFile(new Path("/test/0727/"),new Path("D:\\linuxvmware\\hadooptest2\\src\\main\\resources\\"));
     //   }


        //上传文件

        String srcFilePath = "C:\\Users\\86132\\Desktop\\0715.txt";
        String remoteFilePath = "/test/0727/";
        fs.copyFromLocalFile(new Path(srcFilePath),new Path(remoteFilePath));

        //删除文件
        //fs.delete(new Path("/root/data/"),true);


        //查看文件列表
     //   RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path("/root/data/"), true);
     //   while (fileList.hasNext()){
     ///       System.out.println(fileList.next().toString());
     //   }

        /*//使用流的方式写hdfs文件
        FSDataOutputStream fsDataOutputStream = fs.append(new Path("/root/data/student2.txt"));
        FileInputStream fileinputStream = new FileInputStream("C:\\Users\\86132\\Desktop\\0715.txt");
        byte[] buffer = new byte[1024*1024];
        int read = 0;
        while ((read=fileinputStream.read(buffer)) > 0){
            fsDataOutputStream.write(buffer,0,read);
        }
        //或者使用IOUtil的方式
        InputStream inputStream = new BufferedInputStream(fileinputStream);
        IOUtils.copyBytes(inputStream,fsDataOutputStream,conf);
        //关闭流
        fileinputStream.close();
        fsDataOutputStream.close();

        //使用流的方式下载文件
        FileOutputStream fileOutputStream = new FileOutputStream("./test.txt");
        FSDataInputStream fsDataInputStream = fs.open(new Path("/root/data/student2.txt"));
        byte[] buffer2 = new byte[1024*1024];
        int read2 = 0;
        while ((read2 = fsDataInputStream.read(buffer2)) > 0){
            fileOutputStream.write(buffer2,0,read2);
        }
        //或者使用IOUtils的方式
        OutputStream outputStream = new BufferedOutputStream(fileOutputStream);
        IOUtils.copyBytes(fsDataInputStream,outputStream,conf);

        fileOutputStream.close();
        fsDataInputStream.close();


        fs.close();
*/
    }
}


